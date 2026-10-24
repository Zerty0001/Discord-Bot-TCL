import os
import re
import html
import json
import asyncio
import hashlib
import logging
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes
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

# Lignes TCL valides (819 codes, source : Liste_pictogrammes_lignes.csv de Grand Lyon).
# Sert à l'autocomplete et à la validation de /sub et /traffic.
# Tant que c'est vide, /sub et /traffic acceptent n'importe quel nom sans vérifier.
VALID_LINES: set[str] = set()
# Métro (4)
VALID_LINES.update("""
A B C D
""".split())
# Funiculaire (2)
VALID_LINES.update("""
F1 F2
""".split())
# Tram (et Rhônexpress) (11)
VALID_LINES.update("""
RX T1 T2 T3 T4 T5 T6 T7 T9 T10 T36
""".split())
# TramBus (2)
VALID_LINES.update("""
TB11 TB12
""".split())
# Lignes Chrono (30)
VALID_LINES.update("""
C1 C2 C5 C6 C8 C9 C10 C12 C13 C14 C15 C16 C17 C18 C20 C20EX C21 C22 C22EX C23 C24 C25 C26 C27 C200
C201 C202 C203 C204 C205
""".split())
# Pleine Lune (4)
VALID_LINES.update("""
PL1 PL2 PL3 PL4
""".split())
# Navettes (15)
VALID_LINES.update("""
N180 N181 N182 N183 N184 N185 N186 N190 N191 N192 N195 N196 N197 N198 NAVI1
""".split())
# Bus (146)
VALID_LINES.update("""
31 32 33 34 35 36 37 38 39 40 41 43 44 45 46 49 50 52 52EX 54 55 57 59 60 61 62 63 64 65 66 67 68 69
70 71 72 76 77 78 79 80 81 82 84 85 86 87 88 89 89EX 90 93 95 96 97 98 98EX 99EX 102 103 104A 104B
105 106 106EX 107 108 109 110 110EX 111 112 114 115 116 117 118 119 120 121 122 123 124 125 126 127
128 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153
154 155 156 161 164 165 211 212 213 214 215 216 217 218 219 220 222 230 231 232 235 236 237 238 239
240 241 243 245 247 248 265 285 A32 A71
""".split())
# Bus relais (17)
VALID_LINES.update("""
BR60 BRF1 BRF2 BRMA BRMB BRMC BRMD BRT1 BRT2 BRT3 BRT4 BRT5 BRT6 BRT7 BRT9 BRT10 BRTRX
""".split())
# Lignes JD (588)
VALID_LINES.update("""
JD300 JD301 JD302 JD304 JD305 JD306 JD308 JD311 JD312 JD313 JD314 JD315 JD316 JD317 JD320 JD321
JD322 JD323 JD324 JD325 JD326 JD327 JD328 JD329 JD330 JD331 JD332 JD333 JD334 JD336 JD337 JD338
JD342 JD343 JD344 JD345 JD346 JD347 JD348 JD349 JD350 JD351 JD352 JD353 JD354 JD358 JD359 JD360
JD361 JD362 JD363 JD365 JD366 JD367 JD368 JD369 JD371 JD372 JD373 JD374 JD375 JD376 JD377 JD378
JD379 JD381 JD382 JD383 JD385 JD386 JD387 JD388 JD389 JD391 JD392 JD393 JD394 JD395 JD397 JD398
JD399 JD400 JD402 JD403 JD404 JD405 JD406 JD407 JD408 JD409 JD410 JD411 JD412 JD413 JD414 JD415
JD416 JD417 JD418 JD419 JD420 JD421 JD422 JD423 JD424 JD425 JD426 JD427 JD428 JD429 JD430 JD434
JD435 JD436 JD437 JD438 JD439 JD441 JD442 JD443 JD444 JD445 JD446 JD447 JD448 JD449 JD450 JD451
JD452 JD453 JD454 JD455 JD456 JD457 JD458 JD459 JD460 JD461 JD462 JD464 JD465 JD466 JD467 JD469
JD470 JD471 JD473 JD474 JD476 JD477 JD478 JD479 JD480 JD481 JD482 JD483 JD489 JD490 JD491 JD492
JD493 JD494 JD495 JD496 JD497 JD498 JD499 JD500 JD501 JD502 JD503 JD504 JD505 JD506 JD507 JD508
JD509 JD510 JD511 JD512 JD513 JD514 JD515 JD517 JD518 JD519 JD520 JD522 JD523 JD524 JD525 JD526
JD527 JD528 JD529 JD530 JD531 JD532 JD533 JD534 JD537 JD538 JD539 JD540 JD541 JD542 JD543 JD544
JD545 JD546 JD547 JD548 JD549 JD550 JD551 JD552 JD554 JD555 JD556 JD557 JD558 JD559 JD560 JD561
JD562 JD563 JD564 JD565 JD566 JD567 JD568 JD569 JD573 JD574 JD575 JD576 JD577 JD578 JD579 JD580
JD581 JD582 JD583 JD586 JD587 JD588 JD589 JD590 JD591 JD592 JD593 JD594 JD595 JD596 JD597 JD598
JD600 JD601 JD602 JD603 JD604 JD605 JD606 JD607 JD608 JD609 JD610 JD613 JD615 JD616 JD617 JD618
JD620 JD621 JD622 JD623 JD624 JD626 JD627 JD628 JD629 JD630 JD631 JD632 JD636 JD637 JD638 JD639
JD640 JD642 JD643 JD644 JD645 JD646 JD647 JD648 JD649 JD651 JD652 JD653 JD654 JD655 JD656 JD657
JD658 JD659 JD660 JD661 JD662 JD663 JD664 JD665 JD668 JD669 JD671 JD672 JD673 JD674 JD675 JD676
JD677 JD678 JD679 JD680 JD681 JD682 JD683 JD684 JD685 JD686 JD687 JD688 JD689 JD690 JD691 JD692
JD694 JD695 JD696 JD697 JD698 JD699 JD700 JD701 JD702 JD703 JD704 JD705 JD706 JD708 JD709 JD710
JD711 JD712 JD713 JD714 JD719 JD720 JD721 JD722 JD723 JD724 JD725 JD726 JD727 JD728 JD729 JD734
JD735 JD736 JD737 JD738 JD739 JD741 JD742 JD743 JD744 JD745 JD746 JD750 JD751 JD752 JD753 JD754
JD755 JD756 JD757 JD758 JD759 JD760 JD761 JD762 JD763 JD764 JD765 JD766 JD767 JD768 JD769 JD770
JD771 JD772 JD773 JD774 JD775 JD777 JD778 JD779 JD780 JD781 JD782 JD783 JD784 JD786 JD787 JD788
JD789 JD791 JD792 JD794 JD796 JD797 JD798 JD799 JD800 JD801 JD802 JD803 JD804 JD805 JD806 JD807
JD808 JD809 JD810 JD811 JD812 JD813 JD814 JD815 JD818 JD819 JD820 JD821 JD835 JD836 JD837 JD838
JD839 JD840 JD841 JD842 JD843 JD844 JD845 JD846 JD847 JD848 JD849 JD851 JD853 JD854 JD855 JD856
JD857 JD860 JD861 JD862 JD863 JD864 JD865 JD866 JD867 JD868 JD869 JD870 JD871 JD872 JD873 JD874
JD875 JD876 JD877 JD878 JD879 JD880 JD881 JD882 JD883 JD884 JD885 JD886 JD887 JD890 JD891 JD892
JD893 JD895 JD896 JD897 JD898 JD899 JD900 JD901 JD902 JD903 JD904 JD905 JD908 JD909 JD910 JD912
JD913 JD914 JD917 JD918 JD920 JD921 JD922 JD923 JD924 JD925 JD926 JD927 JD928 JD929 JD931 JD933
JD934 JD935 JD936 JD938 JD940 JD941 JD942 JD943 JD946 JD947 JD948 JD949 JD951 JD952 JD953 JD954
JD955 JD956 JD957 JD958 JD959 JD960 JD961 JD963 JD964 JD965 JD966 JD967 JD968 JD969 JD970 JD971
JD972 JD973 JD974 JD975 JD976 JD977 JD978 JD979 JD980 JD981 JD982 JD983 JD984 JD985 JD986 JD987
JD988 JD989 JD990 JD991 JD992 JD993 JD994 JD995 JD996 JD997 JD998 JD999
""".split())

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
# Funiculaire, relais et JD ajoutés pour le tri de l'autocomplete (/lines les affiche aussi)
_CATEGORY_ORDER = [
    "Métro", "Funiculaire", "Tram", "TramBus", "Ligne Chrono", "Pleine Lune", "Navette",
    "Rhônexpress", "Autres lignes", "Bus relais", "Lignes JD",
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
    if navette_match and 180 <= int(navette_match.group(1)) <= 198:
        return "Navette"
    if ligne in ("F1", "F2"):
        return "Funiculaire"
    if ligne.startswith("BR"):
        return "Bus relais"
    if re.match(r"^JD\d+$", ligne):
        return "Lignes JD"
    return "Autres lignes"


def _natural_sort_key(s: str):
    """Tri 'naturel' (T2 avant T10) en séparant les segments numériques."""
    return [int(tok) if tok.isdigit() else tok for tok in re.split(r"(\d+)", s)]


def _line_sort_key(ligne: str):
    """Tri par type (ordre de _CATEGORY_ORDER), puis naturel dans le type."""
    return (_CATEGORY_ORDER.index(_line_category(ligne)), _natural_sort_key(ligne))


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

async def _fetch_disruptions_uncached() -> list[dict]:
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


# Cache partagé (Discord + Telegram dans le même processus) : un seul appel à
# l'API Grand Lyon même si plusieurs bots/commandes demandent les données.
_FETCH_CACHE_TTL = 30  # secondes
_fetch_cache: dict = {"t": 0.0, "data": None}
_fetch_lock: asyncio.Lock | None = None


async def fetch_disruptions() -> list[dict]:
    global _fetch_lock
    if _fetch_lock is None:
        _fetch_lock = asyncio.Lock()
    async with _fetch_lock:
        if _fetch_cache["data"] is not None and time.monotonic() - _fetch_cache["t"] < _FETCH_CACHE_TTL:
            return list(_fetch_cache["data"])
        data = await _fetch_disruptions_uncached()  # une erreur n'est pas mise en cache
        _fetch_cache.update(t=time.monotonic(), data=data)
        return list(data)


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

        await check_all(channel)
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
        for line in sorted(VALID_LINES, key=_line_sort_key)
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
        ligne = ligne.strip().upper()
        if VALID_LINES and ligne not in VALID_LINES:
            await interaction.followup.send(
                f"❌ La ligne **{ligne}** n'existe pas. Choisis une ligne dans la liste proposée (ex: C1, T2, 224).",
                ephemeral=True,
            )
            return
        lignes_cibles = {ligne}
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


# ======================================================================
#  TELEGRAM (@tcl_info_trafic_bot) — même processus, mêmes données Grand Lyon
#  Variables : TELEGRAM_TOKEN (obligatoire pour activer), TELEGRAM_CHANNEL_ID (optionnel,
#  reçoit toutes les alertes). Sans TELEGRAM_TOKEN, seul Discord démarre.
# ======================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("telegram")
# httpx écrit l'URL complète des requêtes Telegram (donc le token) en INFO : on le coupe
for _name in ("httpx", "httpcore"):
    logging.getLogger(_name).setLevel(logging.WARNING)

TG_STATE_FILE = Path("telegram_state.json")  # distinct de state.json (Discord)
TG_MAX_LEN = 3500  # < 4096 (limite Telegram), marge pour le balisage HTML
_NO_PREVIEW = LinkPreviewOptions(is_disabled=True)


# ---------- État : perturbations déjà vues + abonnements ----------

def tg_load_state() -> dict:
    if TG_STATE_FILE.exists():
        return json.loads(TG_STATE_FILE.read_text(encoding="utf-8"))
    return {}


def tg_save_state(state: dict) -> None:
    TG_STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    _tg_mark_dirty(state)


def get_subs(chat_id: int) -> list[str]:
    subs = tg_load_state().get("subs", {}).get(str(chat_id), [])
    return sorted(subs, key=_natural_sort_key)


def set_subs(chat_id: int, lines: list[str]) -> None:
    state = tg_load_state()
    subs = state.setdefault("subs", {})
    if lines:
        subs[str(chat_id)] = sorted(set(lines), key=_natural_sort_key)
    else:
        subs.pop(str(chat_id), None)
    tg_save_state(state)


# ---------- Persistance des abonnements : message épinglé dans un chat Telegram privé ----------
# Render gratuit efface les fichiers à chaque redéploiement. Les abonnements sont donc aussi
# recopiés dans UN message épinglé d'un chat (canal privé) dont l'id est dans
# TELEGRAM_STORAGE_CHAT_ID ; au démarrage le bot le relit et restaure tout.
# Sans cette variable, les abonnements restent dans le fichier local (non persistants sur Render).

_tg_storage_msg_id: int | None = None
_tg_pushed: str | None = None  # dernier contenu envoyé dans le message de stockage
_tg_dirty = asyncio.Event()


def _storage_chat():
    raw = os.environ.get("TELEGRAM_STORAGE_CHAT_ID", "").strip()
    if not raw:
        return None
    return int(raw) if raw.lstrip("-").isdigit() else raw


def _subs_payload(state: dict) -> str:
    return json.dumps(
        {"v": 1, "subs": state.get("subs", {})},
        ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    )


def _parse_storage(text: str) -> dict | None:
    """Contenu du message épinglé -> dict, ou None si ce n'est pas notre message de stockage."""
    try:
        data = json.loads(text)
    except ValueError:
        return None
    return data if isinstance(data, dict) and data.get("v") == 1 and "subs" in data else None


def _tg_mark_dirty(state: dict) -> None:
    if _tg_storage_msg_id is not None and _subs_payload(state) != _tg_pushed:
        _tg_dirty.set()


async def _tg_create_storage_message(bot, payload: str) -> None:
    global _tg_storage_msg_id, _tg_pushed
    chat_id = _storage_chat()
    msg = await bot.send_message(chat_id, payload)
    try:
        await bot.pin_chat_message(chat_id, msg.message_id, disable_notification=True)
    except TelegramError:
        log.exception("Épinglage impossible : le bot doit pouvoir épingler (canal : droit « Modifier les "
                      "messages des autres » ; groupe : « Épingler des messages ») — sinon les abonnements "
                      "ne seront pas restaurés au prochain démarrage")
    _tg_storage_msg_id = msg.message_id
    _tg_pushed = payload


async def tg_pull_subs(bot) -> None:
    """Au démarrage : restaure les abonnements depuis le message épinglé (ou le crée)."""
    global _tg_storage_msg_id, _tg_pushed
    chat_id = _storage_chat()
    if chat_id is None:
        log.warning("TELEGRAM_STORAGE_CHAT_ID absent : abonnements perdus à chaque redéploiement")
        return
    try:
        chat = await bot.get_chat(chat_id)
        pinned = chat.pinned_message
        data = _parse_storage(pinned.text) if pinned is not None and pinned.text else None
        if data is not None:
            _tg_storage_msg_id = pinned.message_id
            _tg_pushed = pinned.text
            state = tg_load_state()
            state["subs"] = data.get("subs", {})
            tg_save_state(state)
            log.info("Abonnements restaurés : %d chat(s)", len(state["subs"]))
        else:
            await _tg_create_storage_message(bot, _subs_payload(tg_load_state()))
            log.info("Message de stockage créé et épinglé")
    except Exception:
        log.exception("Stockage Telegram indisponible : abonnements non persistants")
        _tg_storage_msg_id = None


async def tg_storage_worker(bot) -> None:
    """Recopie les abonnements dans le message épinglé dès qu'ils changent."""
    global _tg_pushed
    while True:
        await _tg_dirty.wait()
        await asyncio.sleep(1)  # regroupe les modifications rapprochées
        _tg_dirty.clear()
        if _tg_storage_msg_id is None:
            continue
        payload = _subs_payload(tg_load_state())
        if payload == _tg_pushed:
            continue
        if len(payload) > 4000:
            log.error("Abonnements trop volumineux pour un message Telegram (%d car.)", len(payload))
            continue
        try:
            try:
                await bot.edit_message_text(payload, chat_id=_storage_chat(), message_id=_tg_storage_msg_id)
                _tg_pushed = payload
            except BadRequest as error:
                low = str(error).lower()
                if "not modified" in low:
                    _tg_pushed = payload
                elif "not found" in low:
                    await _tg_create_storage_message(bot, payload)
                else:
                    raise
        except Exception:
            log.exception("Sauvegarde des abonnements échouée, nouvel essai dans 30 s")
            await asyncio.sleep(30)
            _tg_dirty.set()


# ---------- Envoi : conversion Discord (**gras**) -> HTML Telegram ----------

def to_html(text: str) -> str:
    text = html.escape(text, quote=False)
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text, flags=re.S)


async def tg_send(bot, chat_id, text: str, reply_markup=None) -> bool:
    """Envoie `text` (format bot.py) en HTML, coupé en morceaux si besoin.
    Renvoie False si le chat est inaccessible (bot bloqué / chat supprimé)."""
    parts = _split_long(text, TG_MAX_LEN) or [text]
    for i, part in enumerate(parts):
        markup = reply_markup if i == len(parts) - 1 else None
        try:
            await bot.send_message(
                chat_id, to_html(part), parse_mode=ParseMode.HTML,
                link_preview_options=_NO_PREVIEW, reply_markup=markup,
            )
        except Forbidden:
            return False
        except BadRequest as error:
            if "chat not found" in str(error).lower():
                return False
            # HTML refusé : on renvoie en texte brut plutôt que de perdre l'alerte
            try:
                await bot.send_message(chat_id, part, link_preview_options=_NO_PREVIEW, reply_markup=markup)
            except TelegramError as error2:
                log.warning("Envoi échoué vers %s : %s", chat_id, error2)
                return False
        except TelegramError as error:
            log.warning("Envoi échoué vers %s : %s", chat_id, error)
            return False
    return True


def sub_button(chat_id: int, ligne: str) -> InlineKeyboardMarkup:
    if ligne in get_subs(chat_id):
        btn = InlineKeyboardButton(f"🔕 Se désabonner de {ligne}", callback_data=f"unsub:{ligne}")
    else:
        btn = InlineKeyboardButton(f"🔔 S'abonner à {ligne}", callback_data=f"sub:{ligne}")
    return InlineKeyboardMarkup([[btn]])


# ---------- Commandes ----------

HELP_TEXT = (
    "🚇 **TCL Info Trafic**\n\n"
    "/menu — menu à boutons (le plus simple)\n"
    "/traffic `C1` — perturbations en cours d'une ligne (sans ligne : tes lignes abonnées)\n"
    "/lines — lignes actuellement perturbées (avec boutons)\n"
    "/sub — voir tes abonnements\n"
    "/sub add `C1 T2` — t'abonner (alertes automatiques)\n"
    "/sub rmv `C1` — te désabonner\n\n"
    "Les alertes sont envoyées dans ce chat : le bot fonctionne aussi dans un groupe."
)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await tg_send(context.bot, update.effective_chat.id, HELP_TEXT, _main_markup())


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await tg_send(context.bot, update.effective_chat.id, MENU_TEXT, _main_markup())


async def cmd_stockage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Diagnostic du stockage des abonnements (commande cachée : /stockage)."""
    chat_id = update.effective_chat.id
    bot_ = context.bot
    cid = _storage_chat()
    lignes = ["🗄️ **Stockage des abonnements**"]

    if cid is None:
        lignes.append("❌ `TELEGRAM_STORAGE_CHAT_ID` est absent ou vide dans les variables d'environnement.")
        await tg_send(bot_, chat_id, "\n".join(lignes))
        return
    lignes.append("✅ Variable `TELEGRAM_STORAGE_CHAT_ID` définie")

    try:
        chat = await bot_.get_chat(cid)
    except TelegramError as error:
        lignes.append(f"❌ Chat introuvable ou inaccessible : {error}")
        lignes.append("Vérifie l'id (il commence par -100 pour un canal) et que le bot est administrateur.")
        await tg_send(bot_, chat_id, "\n".join(lignes))
        return
    lignes.append(f"✅ Chat trouvé : {chat.title or chat.type} ({chat.type})")

    manque = []
    try:
        member = await bot_.get_chat_member(cid, bot_.id)
        if member.status not in ("administrator", "creator"):
            lignes.append(f"❌ Le bot n'est pas administrateur (statut : {member.status})")
        else:
            lignes.append("✅ Le bot est administrateur")
            if chat.type == "channel":
                if getattr(member, "can_post_messages", True) is False:
                    manque.append("Publier des messages")
                if getattr(member, "can_edit_messages", True) is False:
                    manque.append("Modifier les messages des autres (nécessaire pour épingler)")
            elif getattr(member, "can_pin_messages", True) is False:
                manque.append("Épingler des messages")
            if manque:
                lignes.append("❌ Droit(s) manquant(s) : " + " ; ".join(manque))
            else:
                lignes.append("✅ Droits suffisants")
    except TelegramError as error:
        lignes.append(f"❌ Droits du bot illisibles : {error}")

    data = _parse_storage(chat.pinned_message.text) if chat.pinned_message and chat.pinned_message.text else None
    if data is None:
        lignes.append("⚠️ Aucun message de stockage épinglé pour l'instant (créé au prochain démarrage du bot)")
    else:
        lignes.append(f"✅ Message de stockage épinglé trouvé ({len(data.get('subs', {}))} chat(s) abonné(s))")
    lignes.append("✅ Persistance active" if _tg_storage_msg_id is not None
                  else "❌ Persistance inactive dans ce processus (voir les logs de démarrage)")
    await tg_send(bot_, chat_id, "\n".join(lignes))


async def send_traffic(context, chat_id: int, lignes: set[str], single: str | None = None):
    try:
        disruptions = await fetch_disruptions()
    except Exception as error:
        await tg_send(context.bot, chat_id, f"Erreur lors de la récupération des infos trafic : {error}")
        return

    markup = sub_button(chat_id, single) if single else None
    matching = [d for d in disruptions if d["ligne"] in lignes]

    if not matching:
        lignes_str = ", ".join(sorted(lignes, key=_natural_sort_key))
        await tg_send(context.bot, chat_id, f"Aucune perturbation en cours pour : **{lignes_str}** ✅", markup)
        return

    chunks = _format_disruptions_grouped(matching, max_len=TG_MAX_LEN)
    for i, chunk in enumerate(chunks):
        await tg_send(context.bot, chat_id, chunk, markup if i == len(chunks) - 1 else None)


async def cmd_traffic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if context.args:
        ligne = context.args[0].strip().strip(",").upper()
        if VALID_LINES and ligne not in VALID_LINES:
            await tg_send(
                context.bot, chat_id,
                f"❌ La ligne **{ligne}** n'existe pas. Exemples : C1, T2, 60. "
                "Utilise /lines pour voir les lignes perturbées.",
            )
            return
        await send_traffic(context, chat_id, {ligne}, single=ligne)
        return

    abonnements = get_subs(chat_id)
    if not abonnements:
        await tg_send(
            context.bot, chat_id,
            "Tu n'es abonné à aucune ligne. Utilise `/sub add C1` pour t'abonner "
            "ou précise une ligne avec `/traffic C1` (ou /lines).",
        )
        return
    await send_traffic(context, chat_id, set(abonnements))


def parse_sub_args(args: list[str]):
    """['add:C1', 't2', 'rmv', 'c2'] -> (['C1', 'T2'], ['C2']). None si format invalide."""
    raw = " ".join(args).replace(":", " ")
    mode, add, rmv = None, [], []
    for tok in re.split(r"[,\s]+", raw.strip()):
        if not tok:
            continue
        low = tok.lower()
        if low in ("add", "rmv"):
            mode = low
        elif mode == "add":
            add.append(tok.upper())
        elif mode == "rmv":
            rmv.append(tok.upper())
        else:
            return None
    return list(dict.fromkeys(add)), list(dict.fromkeys(rmv))


async def cmd_sub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    parsed = parse_sub_args(context.args)
    if parsed is None:
        await tg_send(context.bot, chat_id, "Format : `/sub add C1 T2` ou `/sub rmv C1`")
        return
    a_ajouter, a_retirer = parsed
    abonnements = get_subs(chat_id)

    if not a_ajouter and not a_retirer:
        if not abonnements:
            await tg_send(context.bot, chat_id, "Tu n'es abonné à aucune ligne. Utilise `/sub add C1` pour t'abonner.")
        else:
            await tg_send(context.bot, chat_id, "🔔 **Tes abonnements**\n" + ", ".join(f"**{l}**" for l in abonnements))
        return

    conflit = sorted(set(a_ajouter) & set(a_retirer), key=_natural_sort_key)
    if conflit:
        await tg_send(context.bot, chat_id, "Une même ligne ne peut pas être dans `add` et `rmv` : **" + "**, **".join(conflit) + "**.")
        return

    messages = []
    courant = set(abonnements)

    if a_ajouter:
        invalides = [l for l in a_ajouter if VALID_LINES and l not in VALID_LINES]
        valides = [l for l in a_ajouter if l not in invalides]
        if valides:
            courant |= set(valides)
            messages.append("Abonné aux lignes : **" + "**, **".join(sorted(valides, key=_natural_sort_key)) + "** ✅")
        if invalides:
            messages.append("Ligne(s) inconnue(s) ignorée(s) : **" + "**, **".join(sorted(invalides, key=_natural_sort_key)) + "**")

    if a_retirer:
        retirees = [l for l in a_retirer if l in courant]
        absentes = [l for l in a_retirer if l not in courant]
        courant -= set(retirees)
        if retirees:
            messages.append("Désabonné des lignes : **" + "**, **".join(sorted(retirees, key=_natural_sort_key)) + "** ✅")
        if absentes:
            messages.append("Tu n'étais pas abonné à : **" + "**, **".join(sorted(absentes, key=_natural_sort_key)) + "**")

    set_subs(chat_id, list(courant))
    await tg_send(context.bot, chat_id, "\n".join(messages))


async def cmd_lines(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_lines(context.bot, update.effective_chat.id)


async def send_lines(bot, chat_id: int) -> None:
    try:
        disruptions = await fetch_disruptions()
    except Exception as error:
        await tg_send(bot, chat_id, f"Erreur lors de la récupération des infos trafic : {error}")
        return

    if not disruptions:
        await tg_send(bot, chat_id, "Aucune perturbation en cours sur le réseau ✅")
        return

    perturbees = {d["ligne"] for d in disruptions}
    grouped: dict[str, list[str]] = {cat: [] for cat in _CATEGORY_ORDER}
    for ligne in perturbees:
        grouped[_line_category(ligne)].append(ligne)

    parts = ["📋 **Lignes actuellement perturbées**"]
    for cat in _CATEGORY_ORDER:
        if grouped[cat]:
            parts.append(f"\n**{cat}**\n" + ", ".join(sorted(grouped[cat], key=_natural_sort_key)))

    # Un bouton par ligne perturbée (max 90, limite Telegram = 100)
    cliquables = [l for l in sorted(perturbees, key=_line_sort_key) if not VALID_LINES or l in VALID_LINES]
    boutons = [InlineKeyboardButton(l, callback_data=f"t:{l}") for l in cliquables[:90]]
    rows = [boutons[i:i + 5] for i in range(0, len(boutons), 5)]
    await tg_send(bot, chat_id, "\n".join(parts), InlineKeyboardMarkup(rows) if rows else None)


# ---------- Menu à boutons ----------

MENU_TEXT = "🚇 **TCL Info Trafic**\nQue veux-tu faire ?"
CAT_EMOJI = {
    "Métro": "🚇", "Funiculaire": "🚡", "Tram": "🚊", "TramBus": "🚎", "Ligne Chrono": "⚡",
    "Pleine Lune": "🌙", "Navette": "⛴️", "Rhônexpress": "✈️", "Autres lignes": "🚌",
    "Bus relais": "🔁", "Lignes JD": "🎒",
}
PAGE_SIZE = 24
_cat_lines_cache: dict[str, list[str]] | None = None


def _cat_lines() -> dict[str, list[str]]:
    """Lignes valides regroupées par type, dans l'ordre d'affichage."""
    global _cat_lines_cache
    if _cat_lines_cache is None:
        grouped: dict[str, list[str]] = {cat: [] for cat in _CATEGORY_ORDER}
        for ligne in VALID_LINES:
            grouped[_line_category(ligne)].append(ligne)
        _cat_lines_cache = {cat: sorted(v, key=_line_sort_key) for cat, v in grouped.items()}
    return _cat_lines_cache


def _btn(label: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(label, callback_data=data)


def _rows(buttons: list, per_row: int) -> list[list]:
    return [buttons[i:i + per_row] for i in range(0, len(buttons), per_row)]


def _main_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [_btn("🚦 Mes lignes", "m:traffic"), _btn("🔔 Mes abonnements", "m:mine")],
        [_btn("📋 Lignes perturbées", "m:lines"), _btn("➕ Ajouter des lignes", "m:add")],
    ])


def screen_main():
    return MENU_TEXT, _main_markup()


def screen_mine(chat_id: int):
    subs = get_subs(chat_id)
    nav = [_btn("➕ Ajouter", "m:add"), _btn("🏠 Menu", "m:main")]
    if not subs:
        return "🔔 **Tes abonnements**\nAucune ligne pour l'instant.", InlineKeyboardMarkup([nav])
    boutons = [_btn(f"❌ {l}", f"r:{l}") for l in subs[:90]]
    return (
        "🔔 **Tes abonnements**\nAppuie sur une ligne pour te désabonner.",
        InlineKeyboardMarkup(_rows(boutons, 4) + [nav]),
    )


def screen_categories(chat_id: int):
    if not VALID_LINES:
        return "Liste des lignes indisponible.", InlineKeyboardMarkup([[_btn("🏠 Menu", "m:main")]])
    subs = set(get_subs(chat_id))
    boutons = []
    for i, cat in enumerate(_CATEGORY_ORDER):
        lignes = _cat_lines()[cat]
        if not lignes:
            continue
        mine = sum(1 for l in lignes if l in subs)
        compteur = f"{mine}/{len(lignes)}" if mine else f"{len(lignes)}"
        boutons.append(_btn(f"{CAT_EMOJI.get(cat, '🚌')} {cat} ({compteur})", f"c:{i}:0"))
    return (
        "➕ **Ajouter des lignes**\nChoisis un type de ligne.",
        InlineKeyboardMarkup(_rows(boutons, 2) + [[_btn("🏠 Menu", "m:main")]]),
    )


def screen_category(chat_id: int, i: int, page: int):
    cat = _CATEGORY_ORDER[i]
    lignes = _cat_lines()[cat]
    pages = max(1, -(-len(lignes) // PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    subs = set(get_subs(chat_id))
    morceau = lignes[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    boutons = [_btn(("✅ " if l in subs else "") + l, f"x:{l}:{i}:{page}") for l in morceau]
    rows = _rows(boutons, 4)
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(_btn("◀️", f"c:{i}:{page - 1}"))
        nav.append(_btn(f"{page + 1}/{pages}", f"c:{i}:{page}"))
        if page < pages - 1:
            nav.append(_btn("▶️", f"c:{i}:{page + 1}"))
        rows.append(nav)
    rows.append([_btn("⬅️ Types", "m:add"), _btn("🏠 Menu", "m:main")])
    titre = f"{CAT_EMOJI.get(cat, '🚌')} **{cat}**" + (f" — page {page + 1}/{pages}" if pages > 1 else "")
    return titre + "\nAppuie sur une ligne pour t'abonner / te désabonner (✅ = abonné).", InlineKeyboardMarkup(rows)


async def _edit_screen(query, screen) -> None:
    text, markup = screen
    try:
        await query.edit_message_text(
            to_html(text), parse_mode=ParseMode.HTML, reply_markup=markup, link_preview_options=_NO_PREVIEW,
        )
    except BadRequest as error:
        if "not modified" not in str(error).lower():
            log.warning("Édition de menu échouée : %s", error)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = (query.data or "").split(":")
    action = parts[0]
    chat_id = query.message.chat_id

    def connue(ligne: str) -> bool:
        return not VALID_LINES or ligne in VALID_LINES

    try:
        if action == "m":
            what = parts[1]
            if what == "main":
                await _edit_screen(query, screen_main())
            elif what == "mine":
                await _edit_screen(query, screen_mine(chat_id))
            elif what == "add":
                await _edit_screen(query, screen_categories(chat_id))
            elif what == "traffic":
                subs = get_subs(chat_id)
                if subs:
                    await send_traffic(context, chat_id, set(subs))
                else:
                    await _edit_screen(query, screen_mine(chat_id))
            elif what == "lines":
                await send_lines(context.bot, chat_id)

        elif action == "c":
            i, page = int(parts[1]), int(parts[2])
            if 0 <= i < len(_CATEGORY_ORDER):
                await _edit_screen(query, screen_category(chat_id, i, page))

        elif action == "x":  # (dés)abonnement depuis une page de catégorie
            ligne, i, page = parts[1], int(parts[2]), int(parts[3])
            if connue(ligne) and 0 <= i < len(_CATEGORY_ORDER):
                courant = set(get_subs(chat_id))
                courant.symmetric_difference_update({ligne})
                set_subs(chat_id, list(courant))
                await _edit_screen(query, screen_category(chat_id, i, page))

        elif action == "r":  # désabonnement depuis « Mes abonnements »
            courant = set(get_subs(chat_id))
            courant.discard(parts[1])
            set_subs(chat_id, list(courant))
            await _edit_screen(query, screen_mine(chat_id))

        elif action == "t" and connue(parts[1]):
            await send_traffic(context, chat_id, {parts[1]}, single=parts[1])

        elif action in ("sub", "unsub") and connue(parts[1]):
            courant = set(get_subs(chat_id))
            courant.add(parts[1]) if action == "sub" else courant.discard(parts[1])
            set_subs(chat_id, list(courant))
            try:
                await query.edit_message_reply_markup(sub_button(chat_id, parts[1]))
            except BadRequest:
                pass  # message identique / trop ancien : sans importance
    except (IndexError, ValueError):
        return  # bouton d'un ancien format : on ignore


# ---------- Alertes automatiques ----------

def _channel_id():
    raw = os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
    if not raw:
        return None
    return int(raw) if raw.lstrip("-").isdigit() else raw


async def tg_initialize_baseline() -> None:
    """À chaque démarrage : mémorise l'existant sans le poster (pas de spam)."""
    try:
        disruptions = await fetch_disruptions()
    except Exception as error:
        log.warning("Initialisation impossible : %s", error)
        return
    state = tg_load_state()
    state["seen_ids"] = list({d["id"] for d in disruptions})
    tg_save_state(state)
    log.info("Initialisation : %d perturbations existantes ignorées.", len(state["seen_ids"]))


async def tg_check_updates(app: Application) -> None:
    try:
        disruptions = await fetch_disruptions()
    except Exception as error:
        log.warning("fetch_disruptions : %s", error)
        return

    state = tg_load_state()
    seen = set(state.get("seen_ids", []))
    subs = state.get("subs", {})
    canal = _channel_id()
    dead: set[str] = set()

    for d in disruptions:
        if d["id"] in seen:
            continue
        seen.add(d["id"])

        cibles = {cid for cid, lignes in subs.items() if d["ligne"] in lignes}
        if canal is not None:
            cibles.add(canal)
        for cid in cibles:
            ok = await tg_send(app.bot, cid, d["message"])
            if not ok and cid in subs:
                dead.add(cid)
            await asyncio.sleep(0.05)

    # Recharge : des /sub ont pu changer pendant les envois
    state = tg_load_state()
    state["seen_ids"] = list(seen)
    for cid in dead:
        state.get("subs", {}).pop(cid, None)
    tg_save_state(state)


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


# ---------- Démarrage (Discord + Telegram dans un seul processus) ----------

tg_app: Application | None = None

TG_COMMANDS = [
    BotCommand("menu", "Menu à boutons"),
    BotCommand("traffic", "Perturbations d'une ligne (ex: /traffic C1)"),
    BotCommand("lines", "Lignes actuellement perturbées"),
    BotCommand("sub", "Gérer tes abonnements (add / rmv)"),
    BotCommand("help", "Aide"),
]


def build_tg_app() -> Application:
    app = Application.builder().token(os.environ["TELEGRAM_TOKEN"]).build()
    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CommandHandler("traffic", cmd_traffic))
    app.add_handler(CommandHandler("sub", cmd_sub))
    app.add_handler(CommandHandler("lines", cmd_lines))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("stockage", cmd_stockage))
    app.add_handler(CallbackQueryHandler(on_button))
    return app


async def check_all(channel: discord.TextChannel) -> None:
    """Un cycle d'alertes : Telegram puis Discord (même réponse Grand Lyon via le cache)."""
    if tg_app is not None:
        try:
            await tg_check_updates(tg_app)
        except Exception:
            log.exception("Erreur côté Telegram (Discord non affecté)")
    await check_traffic_updates(channel)


async def main_async() -> None:
    global tg_app
    threading.Thread(target=_run_web, daemon=True).start()

    if os.environ.get("TELEGRAM_TOKEN"):
        try:
            tg_app = build_tg_app()
            await tg_app.initialize()
            await tg_app.bot.set_my_commands(TG_COMMANDS)
            await tg_pull_subs(tg_app.bot)
            tg_app.bot_data["storage_task"] = asyncio.create_task(tg_storage_worker(tg_app.bot))
            await tg_initialize_baseline()
            await tg_app.start()
            await tg_app.updater.start_polling()
            log.info("Bot Telegram démarré")
        except Exception:
            # ex: token invalide -> on désactive Telegram mais Discord démarre quand même
            log.exception("Telegram désactivé (Discord démarre quand même)")
            tg_app = None

    try:
        await bot.start(DISCORD_TOKEN)
    finally:
        if tg_app is not None:
            await tg_app.updater.stop()
            await tg_app.stop()
            await tg_app.shutdown()


if __name__ == "__main__":
    asyncio.run(main_async())
