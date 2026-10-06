import os
import re
import html
import json
import asyncio
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
import aiohttp
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

load_dotenv()

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
CHANNEL_ID = int(os.environ["CHANNEL_ID"])
GUILD_ID = os.environ.get("GUILD_ID")  # optionnel, accélère la sync des slash commands en dev

GRAND_LYON_USER = os.environ["GRAND_LYON_USER"]
GRAND_LYON_PASSWORD = os.environ["GRAND_LYON_PASSWORD"]
GRAND_LYON_URL = (
    "https://data.grandlyon.com/fr/datapusher/ws/rdata/"
    "tcl_sytral.tclalertetrafic_2/all.json"
    "?maxfeatures=-1&start=1&filename=alertes-trafic-reseau-transports-commun-lyonnais-v2"
)

TZ = ZoneInfo("Europe/Paris")
STATE_FILE = Path("state.json")
POLL_INTERVAL_SECONDS = 60

# Lignes TCL valides — à compléter avec la liste actuelle du réseau
# (tu l'as déjà quelque part vu ton projet de renumérotation Chrono).
# Tant que c'est vide, /sub accepte n'importe quel nom sans vérifier.
VALID_LINES: set[str] = set()

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


# ---------- Persistance de l'état (perturbations déjà postées) ----------

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------- Mise en forme des messages ----------

# href avec guillemets doubles, simples ou sans guillemets : groupes 1/2/3 = URL, groupe 4 = libellé
_A_TAG_RE = re.compile(
    r"""<a\s+[^>]*?href\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))[^>]*>(.*?)</a>""",
    re.IGNORECASE | re.DOTALL,
)
# Libellés de lien "génériques" (ex: "cliquant ici") : on les remplace par l'URL
# elle-même, seule chose auto-cliquable en message classique. Pour un libellé
# plus parlant, on garde "libellé (url)".
_GENERIC_LINK_LABELS = {"ici", "cliquez ici", "cliquant ici", "ce lien", "lien", "là", "la"}

# L'API Grand Lyon envoie "en cliquant ici" en texte brut : le lien a été retiré
# côté source et n'existe dans AUCUN champ. On ne peut donc pas retrouver la
# vraie cible : à toi de dire où pointer, selon le TITRE de la perturbation.
# Clé = morceau du titre (insensible à la casse), valeur = URL ajoutée (cliquable)
# après "cliquant ici". Sans correspondance, rien n'est ajouté.
INFO_LINKS_BY_TITLE: dict[str, str] = {
    # "changement de numérotation": "https://www.tcl.fr/...",
    # "tarification spéciale": "https://www.tcl.fr/...",
}
DEFAULT_INFO_URL = ""  # URL par défaut pour tous les autres titres ("" = aucune)
# (?!\s*\() : ne rien ajouter si une URL suit déjà entre parenthèses.
_CLIQUANT_ICI_RE = re.compile(r"\b(?:cliquant|cliquez)\s+ici\b(?!\s*\()", re.IGNORECASE)
_BR_TAG_RE = re.compile(r'<br\s*/?>', re.IGNORECASE)
_ANY_TAG_RE = re.compile(r'<[^>]+>')
# L'API Grand Lyon ne renvoie pas de vrai HTML : les liens apparaissent en texte
# brut du type "www.rhonexpress.fr", sans "https://" devant, donc Discord ne les
# rend pas cliquables tels quels. On détecte ces domaines nus pour ajouter le
# schéma manquant.
# Le lookbehind évite de retoucher une URL qui a déjà son schéma ("https://www...").
_BARE_DOMAIN_RE = re.compile(r'(?<![/@.\w-])www\.[^\s]+', re.IGNORECASE)


_TRAM_LINES = {f"T{i}" for i in range(1, 11)}  # T1 à T10 (T36 = Car région, volontairement exclu)
_TRAMBUS_LINES = {"TB11", "TB12"}
_CHRONO_RE = re.compile(r"^C(\d+)$")  # C1, C2, ... — à ne pas confondre avec le Métro C (une seule lettre)
_PLEINE_LUNE_RE = re.compile(r"^PL(\d+)$")  # PL1, PL2, ...
_NAVETTE_RE = re.compile(r"^N(\d+)$")  # N180 à N197


_CATEGORY_ORDER = [
    "Métro", "Tram", "TramBus", "Ligne Chrono", "Pleine Lune", "Navette",
    "Rhônexpress", "Autres lignes",
]


def _line_category(ligne: str) -> str:
    """Catégorie d'une ligne (même logique que _format_line_label, mais
    renvoie juste le nom de catégorie — utilisé pour regrouper /lines)."""
    ligne = ligne.upper()

    if len(ligne) == 1 and ligne.isalpha():
        return "Métro"
    if ligne == "RX":
        return "Rhônexpress"
    if ligne in _TRAM_LINES:
        return "Tram"
    if ligne in _TRAMBUS_LINES:
        return "TramBus"
    if _CHRONO_RE.match(ligne):
        return "Ligne Chrono"
    if _PLEINE_LUNE_RE.match(ligne):
        return "Pleine Lune"
    navette_match = _NAVETTE_RE.match(ligne)
    if navette_match and 180 <= int(navette_match.group(1)) <= 197:
        return "Navette"
    return "Autres lignes"


def _natural_sort_key(s: str):
    """Tri 'naturel' (T2 avant T10) en séparant les segments numériques."""
    return [int(tok) if tok.isdigit() else tok for tok in re.split(r"(\d+)", s)]


def _format_line_label(ligne: str) -> str:
    """'A' -> 'Métro A', 'T3' -> 'Tram T3', 'TB11' -> 'TramBus TB11',
    'C2' -> 'Ligne Chrono C2', 'PL1' -> 'Pleine Lune PL1',
    'N185' -> 'Navette N185', 'RX' -> 'Rhônexpress' (mention/recherche restent 'RX'),
    tout le reste -> 'Ligne <code>'."""
    ligne = ligne.upper()

    if len(ligne) == 1 and ligne.isalpha():
        return f"Métro {ligne}"

    if ligne == "RX":
        return "Rhônexpress"

    if ligne in _TRAM_LINES:
        return f"Tram {ligne}"

    if ligne in _TRAMBUS_LINES:
        return f"TramBus {ligne}"

    if _CHRONO_RE.match(ligne):
        return f"Ligne Chrono {ligne}"

    if _PLEINE_LUNE_RE.match(ligne):
        return f"Pleine Lune {ligne}"

    navette_match = _NAVETTE_RE.match(ligne)
    if navette_match and 180 <= int(navette_match.group(1)) <= 197:
        return f"Navette {ligne}"

    return f"Ligne {ligne}"


def _html_to_discord_text(text: str) -> str:
    """Nettoie/formate un champ texte de l'API Grand Lyon pour Discord :
    - convertit les balises <a href="...">libellé</a> (guillemets simples ou
      doubles) : un libellé générique comme "ici" est remplacé par l'URL brute
      (seule chose auto-cliquable en message classique), sinon "libellé (url)" ;
    - ajoute "https://" devant les domaines nus type "www.rhonexpress.fr" pour
      que Discord les rende cliquables ;
    - remplace les <br> et tabulations (utilisées comme séparateurs par l'API)
      par de vrais retours à la ligne ;
    - retire le balisage HTML restant et décode les entités."""
    if not text:
        return ""
    def _link_to_text(m: re.Match) -> str:
        url = (m.group(1) or m.group(2) or m.group(3) or "").strip()
        label = _ANY_TAG_RE.sub("", m.group(4)).strip()
        if not url:
            return label
        if not label or label.lower().strip(" .") in _GENERIC_LINK_LABELS:
            return url
        return f"{label} ({url})"

    text = _A_TAG_RE.sub(_link_to_text, text)
    text = _BR_TAG_RE.sub("\n", text)
    text = _ANY_TAG_RE.sub("", text)
    text = text.replace("\t", "\n")

    def _add_scheme(match: re.Match) -> str:
        url = match.group(0)
        trailing = ""
        while url and url[-1] in ".,;:!?)]}'\"":
            trailing = url[-1] + trailing
            url = url[:-1]
        return f"https://{url}{trailing}"

    text = _BARE_DOMAIN_RE.sub(_add_scheme, text)
    return html.unescape(text).strip()


_MOIS_FR = {
    1: "janvier", 2: "février", 3: "mars", 4: "avril", 5: "mai", 6: "juin",
    7: "juillet", 8: "août", 9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre",
}


_ANNEE_SENTINELLE = 2090  # au-delà, l'API utilise une date lointaine pour dire "indéterminé"


def _format_disruption_datetime(raw: str) -> str:
    """Formate une date Grand Lyon ('YYYY-MM-DD HH:MM:SS') en toutes lettres,
    année incluse : ex. '9 septembre 2026 à 14h30' (convertie à l'heure de Paris).
    Les dates sentinelles très lointaines (ex: année 2100) utilisées par l'API
    pour signifier "pas de fin prévue" sont affichées comme telles."""
    if not raw:
        return "?"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.year >= _ANNEE_SENTINELLE:
            return "indéterminée"
        if dt.tzinfo is not None:
            dt = dt.astimezone(TZ)
        mois = _MOIS_FR[dt.month]
        jour = "1er" if dt.day == 1 else str(dt.day)
        return f"{jour} {mois} {dt.year} à {dt.hour:02d}h{dt.minute:02d}"
    except (ValueError, KeyError):
        return raw


def _add_info_link(titre: str, corps: str) -> str:
    """Ajoute l'URL configurée (INFO_LINKS_BY_TITLE / DEFAULT_INFO_URL) après
    'en cliquant ici', selon le titre de la perturbation."""
    titre_l = titre.lower()
    url = next(
        (u for key, u in INFO_LINKS_BY_TITLE.items() if u and key.lower() in titre_l),
        DEFAULT_INFO_URL,
    )
    if not url:
        return corps
    return _CLIQUANT_ICI_RE.sub(lambda m: f"{m.group(0)} ({url})", corps)


def _build_disruption_body_text(entry: dict) -> str:
    """Partie 'texte' d'une perturbation (titre en gras + message — URLs
    affichées en clair pour que Discord les rende cliquables, les liens
    markdown [texte](url) n'étant cliquables que dans les embeds), SANS les
    dates ni le label de ligne. Sert de clé de fusion : deux perturbations
    au corps strictement identique sont regroupées dans /traffic."""
    titre = _html_to_discord_text(entry.get("titre") or "")
    corps = _add_info_link(titre, _html_to_discord_text(entry.get("message") or ""))

    parts = []
    if titre:
        parts.append(f"**{titre}**")
    if corps:
        parts.append(corps)
    return "\n".join(parts)


def _build_disruption_period_text(entry: dict) -> str:
    """Partie 'dates' d'une perturbation : 'Du X au Y' ou 'Du X — durée indéterminée'."""
    debut = _format_disruption_datetime(entry.get("debut"))
    fin = _format_disruption_datetime(entry.get("fin"))

    if fin == "indéterminée":
        return f"Du {debut} — durée indéterminée"
    return f"Du {debut} au {fin}"


def _build_disruption_detail_text(entry: dict) -> str:
    """Détail complet d'une perturbation (texte + période), SANS le label de
    ligne. Utilisé par le message complet (_build_disruption_text)."""
    body = _build_disruption_body_text(entry)
    periode = _build_disruption_period_text(entry)
    return f"{body}\n{periode}" if body else periode


def _build_disruption_text(entry: dict, ligne: str) -> str:
    """Texte complet d'une perturbation, avec son label de ligne en tête —
    utilisé pour les posts automatiques dans le canal (une perturbation =
    un message)."""
    label = _format_line_label(ligne)
    detail = _build_disruption_detail_text(entry)
    return f"🚦 **{label}**\n{detail}"


# ---------- Récupération des perturbations Grand Lyon ----------

async def fetch_disruptions() -> list[dict]:
    """
    Interroge l'API Grand Lyon (dataset tcl_sytral.tclalertetrafic_2).
    Retourne une liste de dicts : {"id": str, "ligne": str, "message": str}.
    L'id est un hash du contenu stable de la perturbation (pas de vrai id
    unique fourni par l'API) : si le contenu change, l'id change aussi,
    ce qui déclenche un nouveau post.
    """
    auth = aiohttp.BasicAuth(GRAND_LYON_USER, GRAND_LYON_PASSWORD)

    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.get(GRAND_LYON_URL) as response:
            response.raise_for_status()
            payload = await response.json()

    disruptions = []
    for entry in payload.get("values", []):
        # ligne_cli est le SEUL champ fiable pour le code de ligne public.
        # ligne_com peut être un code interne totalement différent et absurde
        # (ex: C204 (client) correspond parfois à 5126 (com) côté Grand Lyon) —
        # on ne s'y fie donc jamais, même en secours.
        ligne = (entry.get("ligne_cli") or "?").upper()

        stable_fields = {
            "type": entry.get("type"),
            "cause": entry.get("cause"),
            "debut": entry.get("debut"),
            "fin": entry.get("fin"),
            "ligne": ligne,  # nom de clé volontairement indépendant du champ API
            "titre": entry.get("titre"),
            "message": entry.get("message"),
        }
        entry_id = hashlib.sha256(
            json.dumps(stable_fields, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        disruptions.append({
            "id": entry_id,
            "ligne": ligne,
            "message": _build_disruption_text(entry, ligne),
            "detail": _build_disruption_detail_text(entry),
            "body": _build_disruption_body_text(entry),
            "periode": _build_disruption_period_text(entry),
            "debut": entry.get("debut") or "",
            "fin": entry.get("fin") or "",
            "last_update": entry.get("last_update_fme") or "",
        })

    return disruptions


# ---------- Gestion des rôles d'abonnement par ligne ----------

async def get_or_create_line_role(guild: discord.Guild, line: str) -> discord.Role:
    role_name = f"TCL-{line}"
    role = discord.utils.get(guild.roles, name=role_name)
    if role is None:
        role = await guild.create_role(
            name=role_name,
            mentionable=True,
            reason=f"Rôle d'abonnement pour la ligne {line}",
        )
    return role


# ---------- Planification (grand écart nocturne) ----------

def is_quiet_period(now: datetime) -> bool:
    weekday = now.weekday()  # Lundi=0 ... Dimanche=6
    if weekday in (5, 6):  # matin de samedi ou dimanche = nuit vendredi/samedi
        return 2 <= now.hour < 5
    return 0 <= now.hour < 5


async def initialize_baseline() -> None:
    """Exécuté une fois à CHAQUE démarrage du bot (pas seulement le tout
    premier lancement) : mémorise les perturbations actuellement en cours
    sans les poster, pour qu'un redémarrage ne spamme jamais le canal avec
    l'existant. Seules les perturbations apparues ou modifiées après ce
    point seront postées par check_traffic_updates."""
    try:
        disruptions = await fetch_disruptions()
    except Exception as error:
        print(f"[fetch_disruptions] erreur à l'initialisation : {error}")
        return

    seen_ids = {d["id"] for d in disruptions}
    save_state({"seen_ids": list(seen_ids)})
    print(f"Initialisation : {len(seen_ids)} perturbations existantes ignorées (pas de spam au démarrage).")


async def poll_loop() -> None:
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID) or await bot.fetch_channel(CHANNEL_ID)

    await initialize_baseline()

    while not bot.is_closed():
        now = datetime.now(TZ)
        if is_quiet_period(now):
            wake_at = now.replace(hour=5, minute=0, second=0, microsecond=0)
            if wake_at <= now:
                wake_at += timedelta(days=1)
            await asyncio.sleep((wake_at - now).total_seconds())
            continue

        await check_traffic_updates(channel)
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def check_traffic_updates(channel: discord.TextChannel) -> None:
    print(f"[{datetime.now(TZ):%Y-%m-%d %H:%M:%S}] Vérification des perturbations TCL...")

    state = load_state()
    seen_ids = set(state.get("seen_ids", []))

    try:
        disruptions = await fetch_disruptions()
    except Exception as error:
        print(f"[fetch_disruptions] erreur : {error}")
        return

    for disruption in disruptions:
        if disruption["id"] in seen_ids:
            continue  # déjà vu, rien de nouveau

        seen_ids.add(disruption["id"])
        role = discord.utils.get(channel.guild.roles, name=f"TCL-{disruption['ligne']}")
        mention = role.mention if role else ""

        await channel.send(
            f"{mention}\n{disruption['message']}".strip(),
            allowed_mentions=discord.AllowedMentions(roles=True),
        )

    save_state({"seen_ids": list(seen_ids)})


# ---------- Slash commands ----------

async def line_autocomplete(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
    if not VALID_LINES:
        return []
    # Discord remplace TOUT le champ par la valeur choisie : l'autocomplétion
    # n'a donc de sens que tant qu'une seule ligne est en cours de saisie
    # (une virgule ou un espace déjà tapés indiquent une saisie multi-lignes,
    # auquel cas on ne propose rien pour ne pas écraser ce qui est déjà tapé).
    if re.search(r"[,\s]", current):
        return []
    current = current.upper()
    return [
        app_commands.Choice(name=line, value=line)
        for line in sorted(VALID_LINES, key=_natural_sort_key)
        if current in line
    ][:25]


def _parse_lines(raw: str | None) -> list[str]:
    """'c1, t2  224' -> ['C1', 'T2', '224'] (majuscules, sans doublon, ordre conservé)."""
    if not raw:
        return []
    return list(dict.fromkeys(l.upper() for l in re.split(r"[,\s]+", raw.strip()) if l))


def _user_subscribed_lines(member: discord.Member) -> list[str]:
    """Lignes auxquelles un membre est abonné (déduites de ses rôles TCL-*)."""
    return sorted(
        (role.name.removeprefix("TCL-") for role in member.roles if role.name.startswith("TCL-")),
        key=_natural_sort_key,
    )


async def subscribed_autocomplete(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
    """Autocomplétion pour rmv: propose uniquement les lignes auxquelles on est abonné."""
    if re.search(r"[,\s]", current):
        return []
    current = current.upper()
    return [
        app_commands.Choice(name=line, value=line)
        for line in _user_subscribed_lines(interaction.user)
        if current in line
    ][:25]


@bot.tree.command(
    name="sub",
    description="Voir tes abonnements (sans option), ou en ajouter (add) / retirer (rmv)",
)
@app_commands.describe(
    add="Ligne(s) à ajouter, séparées par espace ou virgule.",
    rmv="Ligne(s) à retirer, séparées par espace ou virgule.",
)
@app_commands.autocomplete(add=line_autocomplete, rmv=subscribed_autocomplete)
async def subscribe(interaction: discord.Interaction, add: str = None, rmv: str = None):
    a_ajouter = _parse_lines(add)
    a_retirer = _parse_lines(rmv)

    # Sans option : liste des abonnements
    if not a_ajouter and not a_retirer:
        abonnements = _user_subscribed_lines(interaction.user)
        if not abonnements:
            await interaction.response.send_message(
                "Tu n'es abonné à aucune ligne. Utilise `/sub add:C1` pour t'abonner.",
                ephemeral=True,
            )
            return
        await interaction.response.send_message(
            "🔔 **Tes abonnements**\n" + ", ".join(f"**{l}**" for l in abonnements),
            ephemeral=True,
        )
        return

    conflit = sorted(set(a_ajouter) & set(a_retirer), key=_natural_sort_key)
    if conflit:
        await interaction.response.send_message(
            "Une même ligne ne peut pas être dans `add` et `rmv` : **" + "**, **".join(conflit) + "**.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    messages = []

    # --- add: ---
    if a_ajouter:
        invalides = [l for l in a_ajouter if VALID_LINES and l not in VALID_LINES]
        valides = [l for l in a_ajouter if l not in invalides]

        if valides:
            roles = [await get_or_create_line_role(interaction.guild, l) for l in valides]
            await interaction.user.add_roles(*roles)
            messages.append("Abonné aux lignes : **" + "**, **".join(sorted(valides, key=_natural_sort_key)) + "** ✅")
        if invalides:
            messages.append(
                "Ligne(s) inconnue(s) ignorée(s) : **"
                + "**, **".join(sorted(invalides, key=_natural_sort_key))
                + "**"
            )

    # --- rmv: ---
    if a_retirer:
        roles_a_retirer = []
        retirees = []
        absentes = []
        for ligne in a_retirer:
            role = discord.utils.get(interaction.guild.roles, name=f"TCL-{ligne}")
            if role and role in interaction.user.roles:
                roles_a_retirer.append(role)
                retirees.append(ligne)
            else:
                absentes.append(ligne)

        if roles_a_retirer:
            await interaction.user.remove_roles(*roles_a_retirer)
            messages.append("Désabonné des lignes : **" + "**, **".join(sorted(retirees, key=_natural_sort_key)) + "** ✅")
        if absentes:
            messages.append(
                "Tu n'étais pas abonné à : **"
                + "**, **".join(sorted(absentes, key=_natural_sort_key))
                + "**"
            )

    await interaction.followup.send("\n".join(messages), ephemeral=True)


def _split_long(text: str, max_len: int) -> list[str]:
    """Découpe un texte en morceaux <= max_len, en coupant de préférence sur
    un retour à la ligne (à défaut, ou si ce retour est trop tôt et ne
    remplirait presque pas le morceau, à max_len)."""
    parts = []
    while len(text) > max_len:
        cut = text.rfind("\n", 0, max_len)
        if cut < max_len // 2:
            cut = max_len
        parts.append(text[:cut].rstrip())
        text = text[cut:].lstrip("\n")
    if text:
        parts.append(text)
    return parts


def _merge_line_entries(entries: list[dict]) -> list[str]:
    """Fusionne les perturbations d'une même ligne dont le texte (titre +
    message) est strictement identique : le texte n'est écrit qu'une fois,
    suivi de toutes les périodes (triées par date de début, sans doublon)."""
    by_body: dict[str, list[tuple[str, str]]] = {}
    for d in entries:
        by_body.setdefault(d["body"], []).append((d["debut"], d["periode"]))

    items = []
    for i, (body, pairs) in enumerate(by_body.items(), start=1):
        periodes = list(dict.fromkeys(p for _, p in sorted(pairs, key=lambda x: x[0])))
        if len(periodes) == 1:
            dates = periodes[0]
        else:
            dates = "\n".join(f"• {p}" for p in periodes)
        items.append(f"{i}. {body}\n{dates}" if body else f"{i}. {dates}")
    return items


def _pack_items(items: list[str], room: int) -> list[list[str]]:
    """Remplit les messages de façon gloutonne : on met le plus de
    perturbations possible dans un message tant que ça tient dans `room`
    caractères ; dès que la suivante ferait dépasser, elle (et le reste)
    part dans le message suivant. Une perturbation qui dépasse `room` à elle
    seule est d'abord coupée sur ses retours à la ligne."""
    pieces: list[str] = []
    for item in items:
        pieces.extend(_split_long(item, room))

    messages: list[list[str]] = []
    current: list[str] = []
    current_len = 0
    for piece in pieces:
        added = len(piece) + (2 if current else 0)  # 2 = "\n\n" de séparation
        if current and current_len + added > room:
            messages.append(current)
            current, current_len = [piece], len(piece)
        else:
            current.append(piece)
            current_len += added
    if current:
        messages.append(current)
    return messages


def _format_disruptions_grouped(disruptions: list[dict], max_len: int = 1900) -> list[str]:
    """Regroupe les perturbations par ligne sous un en-tête 'Info trafic
    <Ligne>' suivi d'une liste numérotée. Les perturbations au texte
    identique sont fusionnées (dates listées dessous). Si une ligne dépasse
    max_len caractères, elle est répartie sur plusieurs messages numérotés
    '(1/3)', '(2/3)'... Les lignes courtes sont ensuite regroupées entre
    elles pour limiter le nombre de messages (max_len < 2000, limite Discord)."""
    by_ligne: dict[str, list[dict]] = {}
    for d in disruptions:
        by_ligne.setdefault(d["ligne"], []).append(d)

    blocks = []
    for ligne in sorted(by_ligne, key=_natural_sort_key):
        header = f"**Info trafic {_format_line_label(ligne)}**"
        items = _merge_line_entries(by_ligne[ligne])

        # place réservée pour " (12/34)" dans l'en-tête et "\n\n" après lui
        room = max_len - len(header) - 8 - 2
        messages = _pack_items(items, room)

        total = len(messages)
        for n, msg_items in enumerate(messages, start=1):
            head = header if total == 1 else f"{header} ({n}/{total})"
            blocks.append("\n\n".join([head, *msg_items]))

    chunks: list[str] = []
    current = ""
    for block in blocks:
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) > max_len and current:
            chunks.append(current)
            current = block
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


@bot.tree.command(
    name="traffic",
    description="Lire les perturbations en cours d'une ligne, ou de tes lignes abonnées si aucune n'est précisée",
)
@app_commands.describe(ligne="Nom de la ligne, ex: C1, T2, 224 (laisser vide = tes lignes abonnées)")
@app_commands.autocomplete(ligne=line_autocomplete)
async def traffic(interaction: discord.Interaction, ligne: str = None):
    await interaction.response.defer(ephemeral=True)

    if ligne:
        lignes_cibles = {ligne.upper()}
    else:
        # Lignes déduites des rôles TCL-* que possède l'utilisateur
        lignes_cibles = set(_user_subscribed_lines(interaction.user))
        if not lignes_cibles:
            await interaction.followup.send(
                "Tu n'es abonné à aucune ligne. Utilise `/sub add:C1` ou précise une ligne avec `/traffic ligne:`.",
                ephemeral=True,
            )
            return

    try:
        disruptions = await fetch_disruptions()
    except Exception as error:
        await interaction.followup.send(
            f"Erreur lors de la récupération des infos trafic : {error}",
            ephemeral=True,
        )
        return

    matching = [d for d in disruptions if d["ligne"] in lignes_cibles]

    if not matching:
        lignes_str = ", ".join(sorted(lignes_cibles))
        await interaction.followup.send(
            f"Aucune perturbation en cours pour : **{lignes_str}** ✅",
            ephemeral=True,
        )
        return

    chunks = _format_disruptions_grouped(matching)
    for chunk in chunks:
        await interaction.followup.send(chunk, ephemeral=True)


@bot.tree.command(
    name="lines",
    description="Lister les lignes TCL actuellement perturbées, triées par type de ligne",
)
async def lines_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    try:
        disruptions = await fetch_disruptions()
    except Exception as error:
        await interaction.followup.send(
            f"Erreur lors de la récupération des infos trafic : {error}",
            ephemeral=True,
        )
        return

    if not disruptions:
        await interaction.followup.send("Aucune perturbation en cours sur le réseau ✅", ephemeral=True)
        return

    grouped: dict[str, list[str]] = {cat: [] for cat in _CATEGORY_ORDER}
    for ligne in {d["ligne"] for d in disruptions}:
        grouped[_line_category(ligne)].append(ligne)

    parts = ["📋 **Lignes actuellement perturbées**"]
    for cat in _CATEGORY_ORDER:
        if grouped[cat]:
            lignes_str = ", ".join(sorted(grouped[cat], key=_natural_sort_key))
            parts.append(f"\n**{cat}**\n{lignes_str}")

    message = "\n".join(parts)
    if len(message) > 1900:
        # Garde-fou si un jour trop de lignes sont perturbées à la fois pour
        # tenir dans un seul message Discord.
        message = message[:1900] + "\n…"

    await interaction.followup.send(message, ephemeral=True)


@bot.tree.command(name="test_last_alert", description="(test) Poste tout de suite la dernière perturbation TCL mise à jour")
async def test_last_alert(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    disruptions = await fetch_disruptions()
    if not disruptions:
        await interaction.followup.send("Aucune perturbation trouvée.", ephemeral=True)
        return

    latest = max(disruptions, key=lambda d: d["last_update"])
    role = discord.utils.get(interaction.guild.roles, name=f"TCL-{latest['ligne']}")
    mention = role.mention if role else ""

    await interaction.channel.send(
        f"{mention}\n{latest['message']}\n-# (test manuel — dernière mise à jour : {latest['last_update']})".strip(),
        allowed_mentions=discord.AllowedMentions(roles=True),
    )
    await interaction.followup.send("Message de test envoyé ✅", ephemeral=True)


# ---------- Mini serveur web (exigé par Render : port fourni dans la variable PORT) ----------

class _Ping(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


def _run_web() -> None:
    HTTPServer(("0.0.0.0", int(os.environ.get("PORT", 10000))), _Ping).serve_forever()


# ---------- Démarrage ----------

@bot.event
async def on_ready():
    if GUILD_ID:
        guild = discord.Object(id=int(GUILD_ID))
        bot.tree.copy_global_to(guild=guild)
        await bot.tree.sync(guild=guild)
    else:
        await bot.tree.sync()

    print(f"Connecté en tant que {bot.user} — commandes synchronisées")
    bot.loop.create_task(poll_loop())


if __name__ == "__main__":
    threading.Thread(target=_run_web, daemon=True).start()
    bot.run(DISCORD_TOKEN)
