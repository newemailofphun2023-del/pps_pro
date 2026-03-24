import os
import sys
import json
import time
import hashlib
import shutil
import sqlite3
import subprocess
import re
from dataclasses import dataclass, field
from collections import defaultdict, Counter
from typing import List, Dict, Optional

from PyQt5.QtCore import Qt, QThread, pyqtSignal, QUrl, QTimer
from PyQt5.QtGui import QFont, QIcon, QPixmap, QColor, QPainter, QLinearGradient, QPen
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QPushButton, QLineEdit, QFileDialog, QCheckBox, QFrame, QButtonGroup,
    QTableWidget, QTableWidgetItem, QAbstractItemView, QHeaderView, QProgressBar,
    QMessageBox, QTabWidget, QTextEdit, QPlainTextEdit, QListWidget, QListWidgetItem,
    QFormLayout, QSlider, QGroupBox, QComboBox, QSpinBox, QRadioButton, QSplitter,
    QDialog, QListWidget, QListWidgetItem, QInputDialog, QScrollArea
)

try:
    from PyQt5.QtMultimedia import QMediaPlayer, QMediaContent
    HAS_MEDIA = True
except Exception:
    HAS_MEDIA = False
    QMediaPlayer = None
    QMediaContent = None

try:
    from mutagen import File as MutagenFile
    HAS_MUTAGEN = True
except Exception:
    HAS_MUTAGEN = False
    MutagenFile = None

APP_TITLE = 'Phunzaa Performance Studio'
APP_VERSION = 'v5.0'
APP_SUBTITLE = 'Music Management • Live Performance • Karaoke Workflow'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'pps_config.json')
DB_PATH = os.path.join(BASE_DIR, 'pps_data.db')
AUDIO_EXTS = ('.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.wma')

CATEGORIES = [
    'ทุกหมวด', 'เพลงคุณภาพสูง', 'เพลงมาตรฐาน', 'เพลงบิตเรตต่ำ',
    'เพลงความยาวผิดปกติ', 'เพลงซ้ำ/ควรตรวจสอบ', 'ชื่อไฟล์มีปัญหา',
    'ข้อมูลขัดแย้ง', 'ไฟล์เสียจริง', 'Jingle', 'ID Station', 'Spot'
]

LANGUAGE_LABELS = {
    'th': 'ไทย',
    'en': 'English',
}

UI_TEXT = {
    'th': {
        'song_detail': '🎧 Song Detail',
        'performance': '🎤 Performance',
        'karaoke': '🧪 Karaoke Prep',
        'settings': '⚙ Settings',
        'library': '🎵 Library',
        'lyrics': '📝 Lyrics Sync',
        'reports': '📊 Reports',
        'loading': 'Loading interface...\nPreparing music library...\nInitializing performance tools...',
        'perf_mode_normal': 'Normal',
        'perf_mode_karaoke': 'Karaoke',
    },
    'en': {
        'song_detail': '🎧 Song Detail',
        'performance': '🎤 Performance',
        'karaoke': '🧪 Karaoke Prep',
        'settings': '⚙ Settings',
        'library': '🎵 Library',
        'lyrics': '📝 Lyrics Sync',
        'reports': '📊 Reports',
        'loading': 'Loading interface...\nPreparing music library...\nInitializing performance tools...',
        'perf_mode_normal': 'Normal',
        'perf_mode_karaoke': 'Karaoke',
    }
}


@dataclass
class SongRecord:
    file_name: str
    full_path: str
    title: str = ''
    artist: str = ''
    album: str = ''
    genre: str = ''
    year: str = ''
    note: str = ''
    lyrics: str = ''
    chords: str = ''
    sync_lines: str = '[]'
    artwork_path: str = ''
    instrumental_path: str = ''
    vocal_path: str = ''
    duration_text: str = '-'
    bitrate_text: str = '-'
    bitrate_value: int = 0
    duration_seconds: float = 0.0
    decoded_duration_seconds: float = 0.0
    status: str = 'OK'
    analysis_flag: str = 'ปกติ'
    auto_category: str = 'เพลงมาตรฐาน'
    duplicate_group: str = '-'
    keep: bool = False
    is_selected: bool = False
    key_text: str = '-'
    bpm_text: str = '-'
    capo_text: str = '-'
    size_bytes: int = 0
    remark: str = '-'
    hash_md5: str = ''
    issue_flags: str = ''
    sample_rate: int = 0
    channels: int = 0
    normalized_name: str = ''
    suggestion: str = ''

class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.init_db()

    def init_db(self):
        cur = self.conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS song_meta (
                full_path TEXT PRIMARY KEY,
                title TEXT DEFAULT '', artist TEXT DEFAULT '', album TEXT DEFAULT '', genre TEXT DEFAULT '', year TEXT DEFAULT '',
                note TEXT DEFAULT '', lyrics TEXT DEFAULT '', chords TEXT DEFAULT '', sync_lines TEXT DEFAULT '[]', artwork_path TEXT DEFAULT '',
                instrumental_path TEXT DEFAULT '', vocal_path TEXT DEFAULT '', key_text TEXT DEFAULT '-', bpm_text TEXT DEFAULT '-', capo_text TEXT DEFAULT '-'
            )
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT DEFAULT ''
            )
        ''')
        self.conn.commit()

    def get_song(self, full_path: str) -> dict:
        cur = self.conn.cursor()
        cur.execute('SELECT title,artist,album,genre,year,note,lyrics,chords,sync_lines,artwork_path,instrumental_path,vocal_path,key_text,bpm_text,capo_text FROM song_meta WHERE full_path=?', (full_path,))
        row = cur.fetchone()
        if not row:
            return {}
        keys = ['title','artist','album','genre','year','note','lyrics','chords','sync_lines','artwork_path','instrumental_path','vocal_path','key_text','bpm_text','capo_text']
        return dict(zip(keys, row))

    def save_song(self, rec: SongRecord):
        cur = self.conn.cursor()
        cur.execute('''
            INSERT INTO song_meta(full_path,title,artist,album,genre,year,note,lyrics,chords,sync_lines,artwork_path,instrumental_path,vocal_path,key_text,bpm_text,capo_text)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(full_path) DO UPDATE SET
                title=excluded.title, artist=excluded.artist, album=excluded.album, genre=excluded.genre, year=excluded.year,
                note=excluded.note, lyrics=excluded.lyrics, chords=excluded.chords, sync_lines=excluded.sync_lines,
                artwork_path=excluded.artwork_path, instrumental_path=excluded.instrumental_path, vocal_path=excluded.vocal_path,
                key_text=excluded.key_text, bpm_text=excluded.bpm_text, capo_text=excluded.capo_text
        ''', (rec.full_path, rec.title, rec.artist, rec.album, rec.genre, rec.year, rec.note, rec.lyrics, rec.chords, rec.sync_lines,
              rec.artwork_path, rec.instrumental_path, rec.vocal_path, rec.key_text, rec.bpm_text, rec.capo_text))
        self.conn.commit()

    def get_setting(self, key: str, default: str = '') -> str:
        cur = self.conn.cursor()
        cur.execute('SELECT value FROM app_settings WHERE key=?', (key,))
        row = cur.fetchone()
        return row[0] if row else default

    def set_setting(self, key: str, value: str):
        cur = self.conn.cursor()
        cur.execute('INSERT INTO app_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, value))
        self.conn.commit()


def load_config() -> dict:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_config(cfg: dict):
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def resource_path(name: str) -> str:
    return os.path.join(BASE_DIR, name)


def get_app_icon() -> QIcon:
    for fn in ['icon.ico', 'icon.png']:
        path = resource_path(fn)
        if os.path.exists(path):
            return QIcon(path)
    pix = QPixmap(128, 128)
    pix.fill(Qt.transparent)
    p = QPainter(pix)
    p.setRenderHint(QPainter.Antialiasing)
    grad = QLinearGradient(0, 0, 128, 128)
    grad.setColorAt(0, QColor('#66798f'))
    grad.setColorAt(1, QColor('#2c3748'))
    p.setBrush(grad)
    p.setPen(QPen(QColor('#a8b7c9'), 2))
    p.drawRoundedRect(6, 6, 116, 116, 22, 22)
    p.setPen(QColor('white'))
    p.setFont(QFont('Segoe UI', 36, QFont.Bold))
    p.drawText(pix.rect(), Qt.AlignCenter, 'PPS')
    p.end()
    return QIcon(pix)


def get_program_icon_pixmap(size: int = 256) -> QPixmap:
    icon = get_app_icon()
    return icon.pixmap(size, size)


def get_splash_pixmap() -> QPixmap:
    path = resource_path('splash.png')
    if os.path.exists(path):
        pix = QPixmap(path)
        if not pix.isNull():
            return pix
    pix = QPixmap(1200, 680)
    p = QPainter(pix)
    grad = QLinearGradient(0, 0, 1200, 680)
    grad.setColorAt(0, QColor('#10161f'))
    grad.setColorAt(1, QColor('#263140'))
    p.fillRect(pix.rect(), grad)
    p.setPen(QColor('white'))
    p.setFont(QFont('Segoe UI', 28, QFont.Bold))
    p.drawText(70, 220, APP_TITLE)
    p.setFont(QFont('Segoe UI', 13))
    p.drawText(72, 260, f'{APP_SUBTITLE}  •  {APP_VERSION}')
    p.end()
    return pix


def format_bytes(size: int) -> str:
    if size < 1024:
        return f'{size} B'
    kb = size / 1024
    if kb < 1024:
        return f'{kb:.0f} KB'
    mb = kb / 1024
    if mb < 1024:
        return f'{mb:.2f} MB'
    gb = mb / 1024
    return f'{gb:.2f} GB'


def format_duration(seconds: float) -> str:
    if not seconds:
        return '-'
    sec = int(seconds)
    return f'{sec//60:02d}:{sec%60:02d}'


def md5_of_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 512), b''):
            h.update(chunk)
    return h.hexdigest()



def normalize_filename(name: str) -> str:
    base = os.path.splitext(os.path.basename(name))[0].lower()
    base = re.sub(r'\[[^\]]*\]', ' ', base)
    base = re.sub(r'\((?:copy|\d+)\)', ' ', base, flags=re.I)
    base = re.sub(r'\bcopy\b', ' ', base, flags=re.I)
    base = re.sub(r'[_\-]+', ' ', base)
    base = re.sub(r'\s+', ' ', base).strip()
    return base


def ffprobe_audio_info(path: str) -> dict:
    info = {
        'ok': False,
        'duration_seconds': 0.0,
        'bitrate_value': 0,
        'sample_rate': 0,
        'channels': 0,
        'header_missing': False,
        'stderr': '',
    }
    try:
        cmd = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration,bit_rate',
            '-show_entries', 'stream=sample_rate,channels,codec_type',
            '-of', 'json', path
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)
        stderr = (proc.stderr or '').strip()
        info['stderr'] = stderr
        if 'Header missing' in stderr or 'header missing' in stderr:
            info['header_missing'] = True
        payload = json.loads(proc.stdout or '{}')
        fmt = payload.get('format', {}) or {}
        info['duration_seconds'] = float(fmt.get('duration') or 0.0)
        try:
            info['bitrate_value'] = int(float(fmt.get('bit_rate') or 0))
        except Exception:
            info['bitrate_value'] = 0
        for stream in payload.get('streams', []) or []:
            if stream.get('codec_type') == 'audio':
                try:
                    info['sample_rate'] = int(stream.get('sample_rate') or 0)
                except Exception:
                    info['sample_rate'] = 0
                try:
                    info['channels'] = int(stream.get('channels') or 0)
                except Exception:
                    info['channels'] = 0
                break
        info['ok'] = info['duration_seconds'] > 0 or info['bitrate_value'] > 0 or info['sample_rate'] > 0 or info['channels'] > 0
    except Exception as e:
        info['stderr'] = str(e)
    return info


def classify_name_problem(file_name: str) -> str:
    base = os.path.splitext(os.path.basename(file_name))[0]
    lower = base.lower()
    if re.fullmatch(r'\d+', base.strip()):
        return 'ชื่อเป็นตัวเลขล้วน'
    if len(base.strip()) <= 3:
        return 'ชื่อสั้นเกินไป'
    if '[' in base or ']' in base:
        return 'มีสัญลักษณ์ []'
    if re.search(r'\b(copy|copy\s*\d*)\b', lower) or re.search(r'\(\d+\)', base):
        return 'มี copy/(1)/(2)'
    if re.fullmatch(r'(track|audio|untitled)[ _-]*\d*', lower):
        return 'ชื่อไม่สื่อความหมาย'
    return ''


def detect_content_category(file_name: str, title: str, artist: str = '') -> str:
    pool = f"{file_name} {title} {artist}".lower()
    if any(k in pool for k in ['jingle', 'จิงเกิล']):
        return 'Jingle'
    if any(k in pool for k in ['id station', 'station id', 'stationid', 'ไอดีสถานี', 'id สถานี']):
        return 'ID Station'
    if any(k in pool for k in ['spot', 'สปอต']):
        return 'Spot'
    return ''


def detect_duration_issue(duration: float) -> str:
    if duration <= 0:
        return 'duration = 0'
    if 0 < duration < 90:
        return 'เพลงสั้นผิดปกติ'
    if duration > 480:
        return 'เพลงยาวผิดปกติ'
    return ''


def build_analysis_text(flags: List[str], status: str) -> str:
    if status == 'BROKEN':
        return 'ไฟล์เสียจริง'
    if not flags:
        return 'ปกติ'
    if 'ข้อมูลขัดแย้ง' in flags:
        return 'ข้อมูลขัดแย้ง'
    if 'ชื่อไฟล์มีปัญหา' in flags:
        return 'ชื่อไฟล์มีปัญหา'
    if 'เวลาเพี้ยน' in flags or 'เพลงความยาวผิดปกติ' in flags:
        return 'เวลาเพี้ยน'
    if 'เพลงซ้ำ/ควรตรวจสอบ' in flags:
        return 'เพลงซ้ำ/ควรตรวจสอบ'
    return 'ควรตรวจสอบ'


def detect_category(rec: SongRecord) -> str:
    content_cat = detect_content_category(rec.file_name, rec.title, rec.artist)
    if content_cat:
        return content_cat
    if rec.status == 'BROKEN':
        return 'ไฟล์เสียจริง'
    flags = set((rec.issue_flags or '').split('|')) if rec.issue_flags else set()
    if 'ข้อมูลขัดแย้ง' in flags:
        return 'ข้อมูลขัดแย้ง'
    if 'ชื่อไฟล์มีปัญหา' in flags:
        return 'ชื่อไฟล์มีปัญหา'
    if 'เพลงความยาวผิดปกติ' in flags or 'เวลาเพี้ยน' in flags:
        return 'เพลงความยาวผิดปกติ'
    if 'เพลงซ้ำ/ควรตรวจสอบ' in flags:
        return 'เพลงซ้ำ/ควรตรวจสอบ'
    if rec.bitrate_value >= 320000:
        return 'เพลงคุณภาพสูง'
    if rec.bitrate_value >= 192000:
        return 'เพลงมาตรฐาน'
    if rec.bitrate_value > 0:
        return 'เพลงบิตเรตต่ำ'
    return 'เพลงมาตรฐาน'


def keep_priority(rec: SongRecord):
    lower = rec.file_name.lower()
    copy_like = ('(1)' in lower) or ('copy' in lower) or ('_1' in lower)
    return (1 if copy_like else 0, 0 if rec.status == 'OK' else 1, -rec.bitrate_value, -rec.duration_seconds, len(rec.file_name))


def load_audio_meta(path: str):
    meta = {
        'title': os.path.splitext(os.path.basename(path))[0],
        'artist': '', 'album': '', 'genre': '', 'year': '',
        'duration_seconds': 0.0, 'bitrate_value': 0,
        'decoded_duration_seconds': 0.0,
        'sample_rate': 0, 'channels': 0,
        'is_broken': False, 'broken_reason': '',
        'mutagen_error': '', 'ffprobe_error': '',
        'ffprobe_header_missing': False,
        'source': 'mutagen',
    }
    mutagen_failed = False
    if HAS_MUTAGEN:
        try:
            mf = MutagenFile(path, easy=True)
            if mf is None or getattr(mf, 'info', None) is None:
                raise Exception('No audio info')
            meta['title'] = (mf.get('title') or [meta['title']])[0]
            meta['artist'] = (mf.get('artist') or [''])[0]
            meta['album'] = (mf.get('album') or [''])[0]
            meta['genre'] = (mf.get('genre') or [''])[0]
            meta['year'] = (mf.get('date') or mf.get('year') or [''])[0]
            info = getattr(mf, 'info', None)
            meta['duration_seconds'] = float(getattr(info, 'length', 0) or 0)
            meta['bitrate_value'] = int(getattr(info, 'bitrate', 0) or 0)
            try:
                meta['sample_rate'] = int(getattr(info, 'sample_rate', 0) or 0)
            except Exception:
                meta['sample_rate'] = 0
            try:
                meta['channels'] = int(getattr(info, 'channels', 0) or 0)
            except Exception:
                meta['channels'] = 0
            meta['decoded_duration_seconds'] = meta['duration_seconds']
        except Exception as e:
            mutagen_failed = True
            meta['mutagen_error'] = str(e)
    else:
        mutagen_failed = True
        meta['mutagen_error'] = 'mutagen not available'

    need_ffprobe = mutagen_failed or meta['duration_seconds'] <= 0 or meta['bitrate_value'] <= 0 or meta['sample_rate'] <= 0 or meta['channels'] <= 0
    if need_ffprobe:
        probe = ffprobe_audio_info(path)
        meta['ffprobe_error'] = probe.get('stderr', '')
        meta['ffprobe_header_missing'] = probe.get('header_missing', False)
        meta['source'] = 'ffprobe' if probe.get('ok') else meta['source']
        if meta['duration_seconds'] <= 0 and probe['duration_seconds'] > 0:
            meta['duration_seconds'] = probe['duration_seconds']
        if meta['decoded_duration_seconds'] <= 0 and probe['duration_seconds'] > 0:
            meta['decoded_duration_seconds'] = probe['duration_seconds']
        if meta['bitrate_value'] <= 0 and probe['bitrate_value'] > 0:
            meta['bitrate_value'] = probe['bitrate_value']
        if meta['sample_rate'] <= 0 and probe['sample_rate'] > 0:
            meta['sample_rate'] = probe['sample_rate']
        if meta['channels'] <= 0 and probe['channels'] > 0:
            meta['channels'] = probe['channels']

        broken_reasons = []
        if mutagen_failed:
            broken_reasons.append('mutagen อ่านไม่ได้')
        if meta['duration_seconds'] <= 0:
            broken_reasons.append('duration = 0')
        if meta['bitrate_value'] <= 0:
            broken_reasons.append('bitrate = 0')
        if probe.get('header_missing'):
            broken_reasons.append('Header missing')
        if meta['sample_rate'] <= 0:
            broken_reasons.append('sample rate = 0')
        if meta['channels'] <= 0:
            broken_reasons.append('channels = 0')
        if broken_reasons:
            meta['is_broken'] = True
            meta['broken_reason'] = ', '.join(broken_reasons)
    return meta



def sanitize_filename_component(name: str) -> str:
    name = (name or '').strip()
    name = re.sub(r'[\/:*?"<>|]+', '', name)
    name = re.sub(r'\s+', ' ', name).strip().strip('.')
    return name


def ensure_unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 1
    while True:
        candidate = f"{base} ({i}){ext}"
        if not os.path.exists(candidate):
            return candidate
        i += 1


def is_ffplay_available() -> bool:
    try:
        proc = subprocess.run(['ffplay', '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=8)
        return proc.returncode == 0
    except Exception:
        return False


def generate_suggestion(status, analysis_flag, remark):
    if status == "BROKEN":
        return "แนะนำให้ตรวจ/ลบ"

    if "ซ้ำ" in analysis_flag:
        return "ตรวจแล้วเลือกเก็บไฟล์เดียว"

    if "ชื่อไฟล์" in analysis_flag:
        return "แนะนำให้เปลี่ยนชื่อ"

    if "เวลาเพี้ยน" in analysis_flag:
        return "ควรตรวจฟัง"

    if "ข้อมูลขัดแย้ง" in analysis_flag:
        return "ตรวจสอบข้อมูลเพลง"

    if "ควรตรวจสอบ" in analysis_flag:
        return "ควรตรวจสอบ"

    return "ใช้งานได้"

class ScanWorker(QThread):
    progress = pyqtSignal(int, str)
    finished_scan = pyqtSignal(list, dict)

    def __init__(self, folder: str, db_path: str, detection_enabled: bool = True):
        super().__init__()
        self.folder = folder
        self.db_path = db_path
        self.detection_enabled = detection_enabled

    def run(self):
        files = []
        for root, _, names in os.walk(self.folder):
            for n in names:
                if n.lower().endswith(AUDIO_EXTS):
                    files.append(os.path.join(root, n))
        total = len(files)
        records: List[SongRecord] = []
        if not total:
            self.finished_scan.emit([], {})
            return

        db = DB(self.db_path)
        for i, path in enumerate(files, 1):
            try:
                size = os.path.getsize(path)
                status = 'OK'
                meta = load_audio_meta(path)
                flags = []
                remark_parts = []

                if size == 0:
                    status = 'BROKEN'
                    remark_parts.append('ไฟล์ว่าง')

                if self.detection_enabled and meta.get('is_broken'):
                    status = 'BROKEN'
                    flags.append('ไฟล์เสียจริง')
                    remark_parts.append(meta.get('broken_reason') or 'stream ผิดปกติ')

                name_issue = classify_name_problem(os.path.basename(path))
                if self.detection_enabled and name_issue:
                    flags.append('ชื่อไฟล์มีปัญหา')
                    remark_parts.append(name_issue)

                duration_issue = detect_duration_issue(meta.get('duration_seconds', 0.0))
                if self.detection_enabled and duration_issue and status != 'BROKEN':
                    flags.append('เพลงความยาวผิดปกติ')
                    flags.append('เวลาเพี้ยน')
                    remark_parts.append(duration_issue)

                if self.detection_enabled and meta.get('title') and normalize_filename(meta['title']) != normalize_filename(os.path.basename(path)):
                    flags.append('ข้อมูลขัดแย้ง')
                    remark_parts.append('ชื่อไฟล์กับ title ไม่สอดคล้อง')

                rec = SongRecord(
                    file_name=os.path.basename(path),
                    full_path=path,
                    title=meta['title'],
                    artist=meta['artist'],
                    album=meta['album'],
                    genre=meta['genre'],
                    year=meta['year'],
                    duration_seconds=meta['duration_seconds'],
                    decoded_duration_seconds=meta.get('decoded_duration_seconds', 0.0),
                    duration_text=format_duration(meta['duration_seconds']),
                    bitrate_value=meta['bitrate_value'],
                    bitrate_text=f"{meta['bitrate_value']//1000} kbps" if meta['bitrate_value'] else '-',
                    size_bytes=size,
                    status=status,
                    analysis_flag='',
                    remark=' | '.join(dict.fromkeys(remark_parts)) if remark_parts else '-',
                    sample_rate=meta.get('sample_rate', 0),
                    channels=meta.get('channels', 0),
                    normalized_name=normalize_filename(os.path.basename(path)),
                )
                saved = db.get_song(path)
                if saved:
                    for k, v in saved.items():
                        setattr(rec, k, v)

                if status == 'BROKEN':
                    rec.analysis_flag = 'ไฟล์เสียจริง'
                    rec.issue_flags = '|'.join(dict.fromkeys(flags or ['ไฟล์เสียจริง']))
                else:
                    rec.issue_flags = '|'.join(dict.fromkeys(flags))
                    rec.analysis_flag = build_analysis_text(flags, status)

                rec.suggestion = generate_suggestion(rec.status, rec.analysis_flag, rec.remark)
                records.append(rec)
            except Exception as e:
                rec = SongRecord(
                    file_name=os.path.basename(path), full_path=path, status='BROKEN',
                    analysis_flag='ไฟล์เสียจริง', auto_category='ไฟล์เสียจริง',
                    remark=f'เปิดไฟล์ไม่สำเร็จ: {e}', issue_flags='ไฟล์เสียจริง', suggestion='แนะนำให้ตรวจ/ลบ'
                )
                records.append(rec)
            self.progress.emit(int(i / total * 55), f'กำลังอ่านไฟล์ {i}/{total}')

        groups = defaultdict(list)
        ok_records = [r for r in records if r.status == 'OK']
        for i, rec in enumerate(ok_records, 1):
            try:
                rec.hash_md5 = md5_of_file(rec.full_path)
                groups[rec.hash_md5].append(rec)
            except Exception:
                pass
            self.progress.emit(55 + int(i / max(len(ok_records), 1) * 25), f'กำลังวิเคราะห์ซ้ำแท้ {i}/{len(ok_records)}')

        dup_groups = {}
        gid = 1
        for _, items in groups.items():
            if len(items) > 1:
                group_name = f'G{gid:03d}'
                sorted_items = sorted(items, key=keep_priority)
                for idx, item in enumerate(sorted_items):
                    item.duplicate_group = group_name
                    item.keep = idx == 0
                    item.analysis_flag = 'ซ้ำแท้'
                    item.issue_flags = '|'.join(filter(None, [item.issue_flags, 'เพลงซ้ำ/ควรตรวจสอบ']))
                    item.remark = 'แนะนำให้เก็บ' if item.keep else 'อาจลบได้'
                    item.suggestion = 'ตรวจแล้วเลือกเก็บไฟล์เดียว'
                dup_groups[group_name] = sorted_items
                gid += 1

        name_groups = defaultdict(list)
        for rec in records:
            if rec.status == 'OK':
                name_groups[rec.normalized_name].append(rec)
        for _, items in name_groups.items():
            if len(items) > 1:
                if not any(i.duplicate_group != '-' for i in items):
                    for item in items:
                        if item.analysis_flag == 'ปกติ':
                            item.analysis_flag = 'ควรตรวจสอบ'
                        if 'เพลงซ้ำ/ควรตรวจสอบ' not in item.issue_flags:
                            item.issue_flags = '|'.join(filter(None, [item.issue_flags, 'เพลงซ้ำ/ควรตรวจสอบ']))
                        if item.remark == '-':
                            item.remark = 'ชื่อไฟล์ใกล้เคียงกัน อาจซ้ำ'
                        item.suggestion = 'ตรวจแล้วเลือกเก็บไฟล์เดียว'

        for rec in records:
            rec.auto_category = detect_category(rec)

        self.progress.emit(100, 'สแกนเสร็จแล้ว')
        self.finished_scan.emit(records, dup_groups)

class DemucsWorker(QThread):
    progress = pyqtSignal(int, str)
    one_done = pyqtSignal(int, str, str, str)
    finished_all = pyqtSignal()

    def __init__(self, rows: List[tuple], output_dir: str):
        super().__init__()
        self.rows = rows
        self.output_dir = output_dir

    def run(self):
        total = len(self.rows)
        for idx, (row, path) in enumerate(self.rows, 1):
            base = os.path.splitext(os.path.basename(path))[0]
            song_dir = os.path.join(self.output_dir, base)
            os.makedirs(song_dir, exist_ok=True)
            inst = os.path.join(song_dir, 'instrumental.wav')
            vocal = os.path.join(song_dir, 'vocals.wav')
            msg = 'ไม่สำเร็จ'
            try:
                cmd = ['python', '-m', 'demucs', '--two-stems=vocals', '-o', self.output_dir, path]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                model_dir = os.path.join(self.output_dir, 'htdemucs', base)
                gen_vocal = os.path.join(model_dir, 'vocals.wav')
                gen_other = os.path.join(model_dir, 'no_vocals.wav')
                if os.path.exists(gen_other):
                    shutil.copy2(gen_other, inst)
                if os.path.exists(gen_vocal):
                    shutil.copy2(gen_vocal, vocal)
                msg = 'สร้างแล้ว' if os.path.exists(inst) else 'ไม่สำเร็จ'
            except Exception:
                msg = 'ไม่สำเร็จ'
            self.one_done.emit(row, msg, inst if os.path.exists(inst) else '', vocal if os.path.exists(vocal) else '')
            self.progress.emit(int(idx / total * 100), f'สร้าง Karaoke {idx}/{total}')
        self.finished_all.emit()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = DB(DB_PATH)
        self.cfg = load_config()
        self.records: List[SongRecord] = []
        self.duplicate_groups: Dict[str, List[SongRecord]] = {}
        self.current_record: Optional[SongRecord] = None
        self.current_folder = self.cfg.get('last_folder', '')
        self.current_volume = int(self.cfg.get('volume', 80))
        self.mute_state = False
        self.is_slider_dragging = False
        self.performance_queue: List[SongRecord] = []
        self.current_queue_index = -1
        self.karaoke_global_enabled = False
        self.current_play_path = ''
        self.current_playback_mode = 'Normal'
        self.measured_duration_ms = 0
        self.reported_duration_ms = 0
        self.duration_mismatch_flag = False
        self.current_language = self.db.get_setting('ui_language', 'th') or 'th'
        self.file_detection_enabled = self.db.get_setting('file_detection_enabled', '1') != '0'
        self.ffplay_proc = None
        self.current_play_path = ''
        self.prefer_ffplay_paths = set()

        self.player = QMediaPlayer() if HAS_MEDIA else None
        if self.player:
            self.player.setVolume(self.current_volume)
            self.player.positionChanged.connect(self.on_player_position_changed)
            self.player.durationChanged.connect(self.on_player_duration_changed)
            self.player.mediaStatusChanged.connect(self.on_media_status_changed)
            self.player.stateChanged.connect(self.on_player_state_changed)
            try:
                self.player.error.connect(self.on_qt_player_error)
            except Exception:
                try:
                    self.player.errorOccurred.connect(self.on_qt_player_error)
                except Exception:
                    pass

        self.setWindowTitle(APP_TITLE)
        self.resize(1680, 980)
        self.setMinimumSize(1400, 860)
        self.setWindowIcon(get_app_icon())
        self.setStyleSheet(self.load_styles())
        self.init_ui()
        self.apply_language()
        if self.current_folder:
            self.folder_input.setText(self.current_folder)

    def load_styles(self) -> str:
        return '''
        QMainWindow, QWidget { background:#0d1521; color:#edf3fb; font-family:'Segoe UI'; font-size:14px; }
        QLabel { background:transparent; }
        #topInfoLabel { color:#9fb1c7; font-size:12px; }
        QTabWidget::pane { border-top:1px solid #344760; background:#0c1320; }
        QTabBar::tab {
            background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #1d2938, stop:1 #141d29);
            border:1px solid #33465e; padding:10px 18px; border-top-left-radius:10px; border-top-right-radius:10px; margin-right:4px;
        }
        QTabBar::tab:selected { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #314863, stop:1 #233549); }
        QLineEdit, QTextEdit, QPlainTextEdit, QComboBox, QSpinBox {
            background:#09111b; border:1px solid #30445b; border-radius:12px; padding:8px 10px;
        }
        QPushButton {
            background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #42536b, stop:1 #334254);
            color:white; border:1px solid #546882; border-radius:12px; padding:8px 14px; font-weight:600;
        }
        QPushButton:hover { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #516783, stop:1 #41536a); }
        QPushButton:pressed { background:#2a3647; }
        QPushButton#dangerButton { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #ad5b65, stop:1 #8b4049); border-color:#c67680; }
        QPushButton#secondaryButton { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #2d3c4f, stop:1 #223041); }
        QPushButton[chip="true"] { background:#263445; border:1px solid #4a5d76; border-radius:10px; padding:6px 12px; font-size:13px; }
        QPushButton[chip="true"]:checked { background:#5d7fb1; border:1px solid #8aa8d6; }
        QFrame#panelCard, QGroupBox, QScrollArea > QWidget > QWidget {
            background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #162131, stop:1 #121c2b);
            border:1px solid #33465d; border-radius:14px;
        }
        QGroupBox { margin-top:12px; padding:12px; }
        QGroupBox::title { left:12px; padding:0 8px; color:#dfeaf9; }
        QTableWidget, QListWidget {
            background:#09111a; border:1px solid #31455c; gridline-color:#223044; alternate-background-color:#0f1825;
            selection-background-color:#617899; selection-color:#ffffff;
        }
        QHeaderView::section { background:#2a394d; border:none; padding:8px; font-weight:700; }
        QProgressBar { background:#091018; border:1px solid #344153; border-radius:10px; text-align:center; }
        QProgressBar::chunk { background:#8ca7ca; border-radius:10px; }
        QSlider::groove:horizontal { background:#0b1118; height:8px; border-radius:4px; border:1px solid #2f4054; }
        QSlider::sub-page:horizontal { background:#4ba3ff; border-radius:4px; }
        QSlider::handle:horizontal { background:#e8eef6; width:16px; margin:-6px 0; border-radius:8px; }
        QScrollBar:vertical { background:#111a27; width:13px; margin:2px; border:none; border-radius:6px; }
        QScrollBar::handle:vertical { background:#5d7392; min-height:34px; border-radius:6px; }
        QScrollBar::handle:vertical:hover { background:#7590b5; }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height:0px; background:none; border:none; }
        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background:#111a27; border-radius:6px; }
        QScrollBar:horizontal { background:#111a27; height:13px; margin:2px; border:none; border-radius:6px; }
        QScrollBar::handle:horizontal { background:#5d7392; min-width:34px; border-radius:6px; }
        QScrollBar::handle:horizontal:hover { background:#7590b5; }
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width:0px; background:none; border:none; }
        QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal { background:#111a27; border-radius:6px; }
        '''

    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main = QVBoxLayout(central)
        main.setContentsMargins(10, 8, 10, 10)
        main.setSpacing(6)

        self.tabs = QTabWidget()
        main.addWidget(self.tabs)
        self.tab_library = QWidget()
        self.tab_detail = QWidget()
        self.tab_lyrics = QWidget()
        self.tab_performance = QWidget()
        self.tab_karaoke = QWidget()
        self.tab_reports = QWidget()
        self.tab_settings = QWidget()
        self.tabs.addTab(self.tab_library, '🎵 Library')
        self.tabs.addTab(self.tab_detail, '🎧 Song Detail')
        self.tabs.addTab(self.tab_lyrics, '📝 Lyrics Sync')
        self.tabs.addTab(self.tab_performance, '🎤 Performance')
        self.tabs.addTab(self.tab_karaoke, '🧪 Karaoke Prep')
        self.tabs.addTab(self.tab_reports, '📊 Reports')
        self.tabs.addTab(self.tab_settings, '⚙ Settings')

        self.build_library_tab()
        self.build_detail_tab()
        self.build_lyrics_tab()
        self.build_performance_tab()
        self.build_karaoke_tab()
        self.build_reports_tab()
        self.build_settings_tab()

    # ---------- Library ----------
    def build_library_tab(self):
        self.library_filter_category = 'ทุกหมวด'
        root = QVBoxLayout(self.tab_library)
        root.setContentsMargins(6, 6, 6, 6)
        root.setSpacing(6)

        self.library_title = QLabel('Library')
        self.library_title.setStyleSheet('font-size:22px;font-weight:700;')
        root.addWidget(self.library_title)
        self.library_subtitle = QLabel('Audio Scan • Duplicate • Metadata • Playlist • Performance')
        self.library_subtitle.setObjectName('topInfoLabel')
        root.addWidget(self.library_subtitle)

        top_wrap = QFrame(); top_wrap.setObjectName('panelCard')
        top_layout = QGridLayout(top_wrap)
        top_layout.setContentsMargins(10, 8, 10, 8); top_layout.setHorizontalSpacing(8); top_layout.setVerticalSpacing(6)

        self.folder_input = QLineEdit(); self.folder_input.setPlaceholderText('เลือกโฟลเดอร์คลังเพลง...')
        self.folder_input.setText(self.current_folder)
        self.browse_btn = QPushButton('เลือกโฟลเดอร์'); self.browse_btn.clicked.connect(self.select_folder)
        self.scan_analyze_btn = QPushButton('สแกนและวิเคราะห์'); self.scan_analyze_btn.clicked.connect(self.scan_and_analyze)
        self.search_box = QLineEdit(); self.search_box.setPlaceholderText('ค้นหาชื่อเพลง / ศิลปิน / path'); self.search_box.textChanged.connect(self.apply_filters)
        self.show_found_only = QCheckBox('แสดงเฉพาะผลที่ควรตรวจสอบ'); self.show_found_only.stateChanged.connect(self.apply_filters)
        self.show_selected_only = QCheckBox('แสดงเฉพาะไฟล์ที่เลือก'); self.show_selected_only.stateChanged.connect(self.apply_filters)
        self.clear_btn = QPushButton('ล้างข้อมูล'); self.clear_btn.setObjectName('secondaryButton'); self.clear_btn.clicked.connect(self.clear_all)

        top_layout.addWidget(self.folder_input, 0, 0)
        top_layout.addWidget(self.browse_btn, 0, 1)
        top_layout.addWidget(self.scan_analyze_btn, 0, 2)
        top_layout.addWidget(self.search_box, 0, 3)
        top_layout.addWidget(self.show_found_only, 0, 4)
        top_layout.addWidget(self.show_selected_only, 0, 5)
        top_layout.addWidget(self.clear_btn, 0, 6)
        top_layout.setColumnStretch(0, 3)
        top_layout.setColumnStretch(3, 2)
        root.addWidget(top_wrap)

        filter_wrap = QFrame(); filter_wrap.setObjectName('panelCard')
        filter_layout = QHBoxLayout(filter_wrap)
        filter_layout.setContentsMargins(10, 8, 10, 8); filter_layout.setSpacing(6)
        filter_layout.addWidget(QLabel('กรองหมวด'))
        self.category_button_group = QButtonGroup(self); self.category_button_group.setExclusive(True)
        self.category_buttons = {}
        for cat in CATEGORIES:
            btn = QPushButton(cat); btn.setCheckable(True); btn.setProperty('chip', True); btn.setMinimumHeight(30)
            btn.clicked.connect(lambda checked, c=cat: self.on_library_category_changed(c))
            self.category_button_group.addButton(btn); self.category_buttons[cat] = btn; filter_layout.addWidget(btn)
        filter_layout.addStretch()
        self.set_library_category_chip('ทุกหมวด')
        root.addWidget(filter_wrap)

        action_wrap = QFrame(); action_wrap.setObjectName('panelCard')
        action_layout = QHBoxLayout(action_wrap)
        action_layout.setContentsMargins(10, 8, 10, 8); action_layout.setSpacing(6)
        buttons = [
            ('เลือกผลที่พบ', self.select_found_rows, 'secondaryButton'),
            ('ล้างที่เลือก', self.clear_selected_rows, 'secondaryButton'),
            ('จัด KEEP อัตโนมัติ', self.auto_mark_keep_first, 'secondaryButton'),
            ('เปลี่ยนชื่อไฟล์ตาม Metadata', self.rename_selected_files, 'secondaryButton'),
            ('ส่งเข้าคิว Performance', self.add_selected_to_queue, 'secondaryButton'),
            ('สร้าง Playlist', self.export_playlist_selected, 'secondaryButton'),
            ('ส่งออกรายงาน PDF', self.export_pdf_report, 'secondaryButton'),
            ('ลบไฟล์ที่เลือก', self.delete_selected, 'dangerButton'),
        ]
        self.action_buttons = []
        for txt, fn, obj in buttons:
            b = QPushButton(txt); b.setObjectName(obj); b.setMinimumHeight(38); b.clicked.connect(fn); action_layout.addWidget(b)
            self.action_buttons.append((txt, b))
        action_layout.addStretch()
        root.addWidget(action_wrap)

        self.progress = QProgressBar(); self.progress.setValue(0); self.progress.setMinimumHeight(22); root.addWidget(self.progress)

        summary_wrap = QFrame(); summary_wrap.setObjectName('panelCard')
        summary_layout = QHBoxLayout(summary_wrap); summary_layout.setContentsMargins(10, 6, 10, 6); summary_layout.setSpacing(16)
        self.summary_files = QLabel('ไฟล์ทั้งหมด: 0'); self.summary_found = QLabel('ผลที่พบ: 0'); self.summary_groups = QLabel('กลุ่มไฟล์ซ้ำ: 0'); self.summary_size = QLabel('ขนาดรวม: 0 MB')
        for w in [self.summary_files, self.summary_found, self.summary_groups, self.summary_size]:
            w.setStyleSheet('font-size:13px;font-weight:600;'); summary_layout.addWidget(w)
        summary_layout.addStretch(); root.addWidget(summary_wrap)

        self.table = QTableWidget(0, 13)
        self.table.setHorizontalHeaderLabels(['✓','กลุ่ม','KEEP','ชื่อไฟล์','Title','Artist','ระยะเวลา','Bitrate','สถานะ','ผลวิเคราะห์','หมวด','Key/BPM','ที่อยู่ไฟล์'])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.itemChanged.connect(self.on_table_item_changed)
        self.table.itemSelectionChanged.connect(self.on_library_selection_changed)
        self.table.doubleClicked.connect(self.on_table_double_clicked)
        header = self.table.horizontalHeader()
        for i in range(12): header.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(12, QHeaderView.Stretch)
        self.table.setMinimumHeight(460)
        root.addWidget(self.table, 1)

        player_wrap = QFrame(); player_wrap.setObjectName('panelCard')
        player_layout = QVBoxLayout(player_wrap); player_layout.setContentsMargins(10, 8, 10, 8); player_layout.setSpacing(6)
        player_title = QLabel('Mini Player'); player_title.setStyleSheet('font-size:18px;font-weight:700;'); player_layout.addWidget(player_title)
        ctrl = QHBoxLayout(); ctrl.setSpacing(8)
        self.library_prev_btn = QPushButton('⏮'); self.library_play_pause_btn = QPushButton('▶'); self.library_stop_btn = QPushButton('⏹'); self.library_next_btn = QPushButton('⏭')
        for b in [self.library_prev_btn, self.library_play_pause_btn, self.library_stop_btn, self.library_next_btn]:
            b.setMinimumSize(48, 44); b.setStyleSheet('font-size:20px;font-weight:700;')
        self.library_prev_btn.clicked.connect(self.play_previous_in_table)
        self.library_play_pause_btn.clicked.connect(self.toggle_library_play_pause)
        self.library_stop_btn.clicked.connect(self.stop_preview)
        self.library_next_btn.clicked.connect(self.play_next_in_table)
        self.now_playing_label = QLabel('ยังไม่ได้เลือกไฟล์'); self.now_playing_label.setStyleSheet('font-size:14px;font-weight:600;')
        self.library_mute_btn = QPushButton('🔊'); self.library_mute_btn.setMinimumSize(42, 38); self.library_mute_btn.clicked.connect(self.toggle_mute)
        self.library_volume_slider = QSlider(Qt.Horizontal); self.library_volume_slider.setRange(0,100); self.library_volume_slider.setValue(self.current_volume); self.library_volume_slider.valueChanged.connect(self.on_volume_changed)
        for w in [self.library_prev_btn, self.library_play_pause_btn, self.library_stop_btn, self.library_next_btn]: ctrl.addWidget(w)
        ctrl.addWidget(self.now_playing_label,1); ctrl.addWidget(self.library_mute_btn); ctrl.addWidget(QLabel('Vol')); ctrl.addWidget(self.library_volume_slider)
        player_layout.addLayout(ctrl)
        timeline = QHBoxLayout(); self.library_position_label = QLabel('00:00'); self.library_seek_slider = QSlider(Qt.Horizontal); self.library_seek_slider.setRange(0,0); self.library_seek_slider.sliderPressed.connect(self.on_slider_pressed); self.library_seek_slider.sliderReleased.connect(self.on_library_slider_released); self.library_seek_slider.sliderMoved.connect(self.on_library_slider_moved); self.library_duration_label = QLabel('00:00'); timeline.addWidget(self.library_position_label); timeline.addWidget(self.library_seek_slider,1); timeline.addWidget(self.library_duration_label); player_layout.addLayout(timeline)
        root.addWidget(player_wrap)

        self.status_label = QLabel('พร้อมทำงาน'); self.status_label.setObjectName('topInfoLabel'); root.addWidget(self.status_label)

    def set_library_category_chip(self, category_name: str):
        self.library_filter_category = category_name
        for name, btn in self.category_buttons.items():
            btn.setChecked(name == category_name)

    def on_library_category_changed(self, category_name: str):
        self.set_library_category_chip(category_name)
        if category_name == 'ทุกหมวด':
            self.show_found_only.setChecked(False)
            self.show_selected_only.setChecked(False)
        self.apply_filters()

    def select_folder(self):
        folder = QFileDialog.getExistingDirectory(self, 'เลือกโฟลเดอร์')
        if folder:
            self.current_folder = folder
            self.folder_input.setText(folder)
            self.cfg['last_folder'] = folder
            save_config(self.cfg)

    def scan_and_analyze(self):
        folder = self.folder_input.text().strip()
        if not folder or not os.path.exists(folder):
            QMessageBox.warning(self, 'แจ้งเตือน', 'กรุณาเลือกโฟลเดอร์ที่ถูกต้อง')
            return
        self.current_folder = folder
        self.cfg['last_folder'] = folder
        save_config(self.cfg)
        self.table.blockSignals(True); self.table.setRowCount(0); self.table.blockSignals(False)
        self.records = []; self.progress.setValue(0); self.status_label.setText('กำลังสแกน...')
        self.worker = ScanWorker(folder, DB_PATH, self.file_detection_enabled)
        self.worker.progress.connect(self.on_scan_progress)
        self.worker.finished_scan.connect(self.on_scan_finished)
        self.worker.start()

    def on_scan_progress(self, value: int, msg: str):
        self.progress.setValue(value); self.status_label.setText(msg)

    def on_scan_finished(self, records, duplicate_groups):
        self.records = records; self.duplicate_groups = duplicate_groups
        self.populate_table(); self.update_summary(); self.update_reports(); self.progress.setValue(100)
        self.status_label.setText(f'สแกนเสร็จแล้ว {len(records)} ไฟล์ | พบต้องตรวจสอบ {sum(1 for r in records if r.auto_category in {"เพลงซ้ำ/ควรตรวจสอบ","ชื่อไฟล์มีปัญหา","ข้อมูลขัดแย้ง","ไฟล์เสียจริง","เพลงความยาวผิดปกติ"})} รายการ')

    def category_color(self, category: str) -> QColor:
        colors = {
            'เพลงคุณภาพสูง': QColor('#17324d'),
            'เพลงมาตรฐาน': QColor('#102030'),
            'เพลงบิตเรตต่ำ': QColor('#4a3a15'),
            'เพลงความยาวผิดปกติ': QColor('#3c234d'),
            'เพลงซ้ำ/ควรตรวจสอบ': QColor('#4a3518'),
            'ชื่อไฟล์มีปัญหา': QColor('#3b2f4d'),
            'ข้อมูลขัดแย้ง': QColor('#4d2431'),
            'ไฟล์เสียจริง': QColor('#5a262d'),
            'Jingle': QColor('#163d2f'),
            'ID Station': QColor('#12434a'),
            'Spot': QColor('#3a2f52'),
        }
        return colors.get(category, QColor('#091018'))

    def populate_table(self):
        self.table.blockSignals(True)
        self.table.setRowCount(0)
        for rec in self.records:
            row = self.table.rowCount()
            self.table.insertRow(row)
            checked = QTableWidgetItem()
            checked.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable)
            checked.setCheckState(Qt.Checked if rec.is_selected else Qt.Unchecked)
            self.table.setItem(row, 0, checked)
            vals = [
                rec.duplicate_group,
                'KEEP' if rec.keep else '-',
                rec.file_name,
                rec.title,
                rec.artist,
                rec.duration_text,
                rec.bitrate_text,
                rec.status,
                rec.analysis_flag,
                rec.auto_category,
                f'{rec.key_text}/{rec.bpm_text}',
                rec.full_path
            ]
            for col, v in enumerate(vals, 1):
                self.table.setItem(row, col, QTableWidgetItem(str(v)))
            color = self.category_color(rec.auto_category)
            for c in range(self.table.columnCount()):
                item = self.table.item(row, c)
                if item:
                    item.setBackground(color)
        self.table.blockSignals(False)
        self.apply_filters()

    def update_summary(self):
        self.summary_files.setText(f'ไฟล์ทั้งหมด: {len(self.records)}')
        found_categories = {'เพลงซ้ำ/ควรตรวจสอบ', 'ชื่อไฟล์มีปัญหา', 'ข้อมูลขัดแย้ง', 'ไฟล์เสียจริง', 'เพลงความยาวผิดปกติ'}
        found = sum(1 for r in self.records if r.auto_category in found_categories or r.analysis_flag == 'ซ้ำแท้')
        self.summary_found.setText(f'ผลที่พบ: {found}')
        self.summary_groups.setText(f'กลุ่มไฟล์ซ้ำ: {len(self.duplicate_groups)}')
        self.summary_size.setText(f'ขนาดรวม: {format_bytes(sum(r.size_bytes for r in self.records))}')

    def apply_filters(self):
        query = self.search_box.text().strip().lower()
        category = self.library_filter_category
        found_only = self.show_found_only.isChecked()
        selected_only = self.show_selected_only.isChecked()
        problem_categories = {'เพลงซ้ำ/ควรตรวจสอบ', 'ชื่อไฟล์มีปัญหา', 'ข้อมูลขัดแย้ง', 'ไฟล์เสียจริง', 'เพลงความยาวผิดปกติ'}
        for row, rec in enumerate(self.records):
            visible = True
            pool = f'{rec.file_name} {rec.title} {rec.artist} {rec.full_path} {rec.remark}'.lower()
            if query and query not in pool:
                visible = False
            if category != 'ทุกหมวด' and rec.auto_category != category:
                visible = False
            if found_only and rec.auto_category not in problem_categories and rec.analysis_flag != 'ซ้ำแท้':
                visible = False
            if selected_only and not rec.is_selected:
                visible = False
            self.table.setRowHidden(row, not visible)

    def on_table_item_changed(self, item):
        if item.column() == 0 and 0 <= item.row() < len(self.records):
            self.records[item.row()].is_selected = item.checkState() == Qt.Checked

    def on_library_selection_changed(self):
        row = self.table.currentRow()
        if row < 0 or row >= len(self.records):
            self.now_playing_label.setText('ยังไม่ได้เลือกไฟล์')
            return
        rec = self.records[row]
        self.current_record = rec
        self.now_playing_label.setText(rec.file_name)
        self.load_current_record_into_tabs(rec)

    def on_table_double_clicked(self):
        self.play_selected()

    def select_found_rows(self):
        self.table.blockSignals(True)
        for row, rec in enumerate(self.records):
            should = not self.table.isRowHidden(row)
            if should and rec.keep and rec.analysis_flag == 'ซ้ำแท้':
                should = False
            rec.is_selected = should
            item = self.table.item(row, 0)
            if item:
                item.setCheckState(Qt.Checked if should else Qt.Unchecked)
        self.table.blockSignals(False)
        self.apply_filters()
        self.status_label.setText('เลือกทุกไฟล์ที่มองเห็นแล้ว')

    def clear_selected_rows(self):
        self.table.blockSignals(True)
        for row, rec in enumerate(self.records):
            rec.is_selected = False
            item = self.table.item(row,0)
            if item: item.setCheckState(Qt.Unchecked)
        self.table.blockSignals(False)
        self.apply_filters(); self.status_label.setText('ล้างที่เลือกแล้ว')

    def auto_mark_keep_first(self):
        for _, items in self.duplicate_groups.items():
            for idx, rec in enumerate(sorted(items, key=keep_priority)):
                rec.keep = idx == 0
                rec.remark = 'แนะนำให้เก็บ' if rec.keep else 'อาจลบได้'
        self.populate_table(); self.status_label.setText('จัด KEEP อัตโนมัติแล้ว')

    def rename_selected_files(self):
        count = 0
        failed = []
        for rec in self.records:
            if not rec.is_selected:
                continue
            title = rec.title.strip() or os.path.splitext(rec.file_name)[0]
            artist = rec.artist.strip()
            base = f'{artist} - {title}' if artist else title
            safe = sanitize_filename_component(base)
            if not safe:
                failed.append(rec.file_name)
                continue
            new_path = os.path.join(os.path.dirname(rec.full_path), safe + os.path.splitext(rec.file_name)[1])
            if os.path.abspath(new_path) == os.path.abspath(rec.full_path):
                continue
            new_path = ensure_unique_path(new_path)
            try:
                os.rename(rec.full_path, new_path)
                rec.full_path = new_path
                rec.file_name = os.path.basename(new_path)
                rec.normalized_name = normalize_filename(rec.file_name)
                self.db.save_song(rec)
                count += 1
            except Exception as e:
                failed.append(f'{rec.file_name}: {e}')
        self.populate_table()
        if failed:
            QMessageBox.warning(self, 'เปลี่ยนชื่อไฟล์', 'สำเร็จ {} รายการ\n\nไม่สำเร็จบางรายการ:\n{}'.format(count, '\n'.join(failed[:8])))
        self.status_label.setText(f'เปลี่ยนชื่อไฟล์แล้ว {count} รายการ')

    def export_playlist_selected(self):
        songs = [r for r in self.records if r.is_selected]
        if not songs:
            QMessageBox.information(self, 'แจ้งเตือน', 'ยังไม่ได้เลือกเพลง')
            return
        out, _ = QFileDialog.getSaveFileName(self, 'บันทึก Playlist', 'playlist.m3u', 'M3U Playlist (*.m3u)')
        if not out: return
        if not out.lower().endswith('.m3u'): out += '.m3u'
        with open(out, 'w', encoding='utf-8') as f:
            f.write('#EXTM3U\n')
            for s in songs:
                f.write(f'#EXTINF:{int(s.duration_seconds) if s.duration_seconds else -1},{s.artist} - {s.title}\n{s.full_path}\n')
        QMessageBox.information(self, 'สำเร็จ', f'บันทึก Playlist แล้ว\n{out}')

    def export_pdf_report(self):
        from PyQt5.QtGui import QPdfWriter
        out, _ = QFileDialog.getSaveFileName(self, 'บันทึกรายงาน PDF', 'report.pdf', 'PDF Files (*.pdf)')
        if not out: return
        if not out.lower().endswith('.pdf'): out += '.pdf'
        pdf = QPdfWriter(out)
        p = QPainter(pdf)
        p.setFont(QFont('Segoe UI', 18, QFont.Bold)); p.drawText(60, 80, APP_TITLE)
        p.setFont(QFont('Segoe UI', 10))
        lines = [self.summary_files.text(), self.summary_found.text(), self.summary_groups.text(), self.summary_size.text(), '']
        counts = Counter(r.auto_category for r in self.records)
        for k, v in counts.items(): lines.append(f'{k}: {v}')
        y = 130
        for line in lines: p.drawText(60, y, line); y += 24
        p.end(); QMessageBox.information(self, 'สำเร็จ', f'ส่งออกรายงานแล้ว\n{out}')

    def delete_selected(self):
        songs = [r for r in self.records if r.is_selected]
        if not songs:
            QMessageBox.information(self, 'แจ้งเตือน', 'ยังไม่ได้เลือกไฟล์')
            return
        if QMessageBox.question(self, 'ยืนยัน', f'ต้องการลบ {len(songs)} ไฟล์หรือไม่?', QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return
        backup = os.path.join(BASE_DIR, 'PTS_Deleted_Backup', time.strftime('delete_%Y%m%d_%H%M%S'))
        os.makedirs(backup, exist_ok=True)
        ok = 0
        for s in songs:
            try:
                shutil.copy2(s.full_path, os.path.join(backup, os.path.basename(s.full_path)))
                os.remove(s.full_path)
                ok += 1
            except Exception:
                pass
        QMessageBox.information(self, 'เสร็จสิ้น', f'ลบแล้ว {ok} ไฟล์')
        if self.current_folder: self.scan_and_analyze()

    def clear_all(self):
        self.records = []; self.duplicate_groups = {}; self.table.setRowCount(0); self.progress.setValue(0)
        self.summary_files.setText('ไฟล์ทั้งหมด: 0'); self.summary_found.setText('ผลที่พบ: 0'); self.summary_groups.setText('กลุ่มไฟล์ซ้ำ: 0'); self.summary_size.setText('ขนาดรวม: 0 MB')
        self.status_label.setText('ล้างข้อมูลแล้ว')

    # ---------- Detail ----------
    def build_detail_tab(self):
        root = QHBoxLayout(self.tab_detail)
        left = QVBoxLayout(); right = QVBoxLayout()
        self.artwork_label = QLabel(); self.artwork_label.setFixedSize(260, 260); self.artwork_label.setAlignment(Qt.AlignCenter); self.artwork_label.setStyleSheet('background:#091018;border:1px solid #344153;border-radius:16px;font-size:40px;font-weight:700;')
        self.artwork_label.setPixmap(get_program_icon_pixmap(220))
        left.addWidget(self.artwork_label)
        self.load_artwork_btn = QPushButton('เลือกรูปศิลปิน / ปกอัลบั้ม / โลโก้'); self.load_artwork_btn.clicked.connect(self.choose_artwork)
        left.addWidget(self.load_artwork_btn); left.addStretch(); root.addLayout(left)
        form_group = QGroupBox('รายละเอียดเพลง')
        form = QFormLayout(form_group)
        self.detail_title = QLineEdit(); self.detail_artist = QLineEdit(); self.detail_album = QLineEdit(); self.detail_genre = QLineEdit(); self.detail_year = QLineEdit(); self.detail_key = QLineEdit(); self.detail_bpm = QLineEdit(); self.detail_capo = QLineEdit(); self.detail_path = QLineEdit(); self.detail_path.setReadOnly(True); self.detail_note = QPlainTextEdit(); self.detail_note.setFixedHeight(120)
        for lbl, widget in [('Title', self.detail_title), ('Artist', self.detail_artist), ('Album', self.detail_album), ('Genre', self.detail_genre), ('Year', self.detail_year), ('Key', self.detail_key), ('BPM', self.detail_bpm), ('Capo', self.detail_capo), ('Path', self.detail_path), ('Note', self.detail_note)]: form.addRow(lbl, widget)
        right.addWidget(form_group)
        btns = QHBoxLayout(); save = QPushButton('บันทึกข้อมูลเพลง'); save.clicked.connect(self.save_current_song); btns.addWidget(save); rename = QPushButton('เปลี่ยนชื่อไฟล์นี้'); rename.clicked.connect(self.rename_current_song); btns.addWidget(rename); btns.addStretch(); right.addLayout(btns)
        root.addLayout(right, 1)

    def load_current_record_into_tabs(self, rec: SongRecord):
        self.detail_title.setText(rec.title); self.detail_artist.setText(rec.artist); self.detail_album.setText(rec.album); self.detail_genre.setText(rec.genre); self.detail_year.setText(rec.year); self.detail_key.setText(rec.key_text); self.detail_bpm.setText(rec.bpm_text); self.detail_capo.setText(rec.capo_text); self.detail_path.setText(rec.full_path); self.detail_note.setPlainText(rec.note)
        self.lyrics_title_label.setText(f'{rec.title or rec.file_name} | {rec.artist}')
        self.lyrics_editor.setPlainText(rec.lyrics)
        self.chords_editor.setPlainText(rec.chords)
        self.performance_title.setText(f'{rec.title or rec.file_name} - {rec.artist}'.strip(' -'))
        self.performance_meta.setText(f'Key: {rec.key_text} | BPM: {rec.bpm_text} | Mode: {self.current_playback_mode}')
        self.performance_text.setPlainText(self.compose_display_text(rec))
        if rec.artwork_path and os.path.exists(rec.artwork_path):
            pix = QPixmap(rec.artwork_path).scaled(250, 250, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            self.artwork_label.setPixmap(pix)
        else:
            self.artwork_label.setText('')
            self.artwork_label.setPixmap(get_program_icon_pixmap(220))
        self.sync_list.clear()
        try:
            lines = json.loads(rec.sync_lines or '[]')
            for obj in lines:
                self.sync_list.addItem(f"{obj.get('time','00:00')}  {obj.get('text','')}")
        except Exception:
            pass

    def choose_artwork(self):
        if not self.current_record:
            QMessageBox.information(self, 'แจ้งเตือน', 'กรุณาเลือกเพลงก่อน')
            return
        path, _ = QFileDialog.getOpenFileName(self, 'เลือกรูป', '', 'Images (*.png *.jpg *.jpeg *.webp)')
        if not path: return
        self.current_record.artwork_path = path
        self.load_current_record_into_tabs(self.current_record)

    def save_current_song(self):
        if not self.current_record:
            QMessageBox.information(self, 'แจ้งเตือน', 'กรุณาเลือกเพลงก่อน')
            return
        r = self.current_record
        r.title = self.detail_title.text().strip(); r.artist = self.detail_artist.text().strip(); r.album = self.detail_album.text().strip(); r.genre = self.detail_genre.text().strip(); r.year = self.detail_year.text().strip(); r.key_text = self.detail_key.text().strip() or '-'; r.bpm_text = self.detail_bpm.text().strip() or '-'; r.capo_text = self.detail_capo.text().strip() or '-'; r.note = self.detail_note.toPlainText().strip(); r.lyrics = self.lyrics_editor.toPlainText(); r.chords = self.chords_editor.toPlainText(); r.sync_lines = json.dumps(self.collect_sync_lines(), ensure_ascii=False); self.db.save_song(r); self.populate_table(); self.load_current_record_into_tabs(r); self.status_label.setText('บันทึกข้อมูลเพลงแล้ว')

    def rename_current_song(self):
        if not self.current_record:
            QMessageBox.information(self, 'แจ้งเตือน', 'กรุณาเลือกเพลงก่อน')
            return
        rec = self.current_record
        old_path = rec.full_path
        old_dir = os.path.dirname(old_path)
        old_ext = os.path.splitext(old_path)[1]
        old_base = os.path.splitext(os.path.basename(old_path))[0]
        new_base, ok = QInputDialog.getText(self, 'เปลี่ยนชื่อไฟล์', 'ชื่อไฟล์ใหม่:', text=old_base)
        if not ok:
            return
        safe = sanitize_filename_component(new_base)
        if not safe:
            QMessageBox.warning(self, 'แจ้งเตือน', 'ชื่อไฟล์ใหม่ไม่ถูกต้อง')
            return
        new_path = ensure_unique_path(os.path.join(old_dir, safe + old_ext))
        try:
            if os.path.abspath(new_path) != os.path.abspath(old_path):
                os.rename(old_path, new_path)
                rec.full_path = new_path
                rec.file_name = os.path.basename(new_path)
                rec.normalized_name = normalize_filename(rec.file_name)
                self.db.save_song(rec)
                self.populate_table()
                self.load_current_record_into_tabs(rec)
            self.status_label.setText(f'เปลี่ยนชื่อไฟล์แล้ว: {rec.file_name}')
        except Exception as e:
            QMessageBox.critical(self, 'เปลี่ยนชื่อไม่สำเร็จ', str(e))

    # ---------- Lyrics Sync ----------
    def build_lyrics_tab(self):
        root = QVBoxLayout(self.tab_lyrics)
        self.lyrics_title_label = QLabel('ยังไม่ได้เลือกเพลง'); self.lyrics_title_label.setStyleSheet('font-size:18px;font-weight:700;'); root.addWidget(self.lyrics_title_label)
        splitter = QSplitter(Qt.Horizontal)
        left = QWidget(); ll = QVBoxLayout(left)
        self.lyrics_editor = QTextEdit(); self.lyrics_editor.setPlaceholderText('เนื้อเพลง'); ll.addWidget(self.lyrics_editor)
        right = QWidget(); rl = QVBoxLayout(right)
        self.chords_editor = QTextEdit(); self.chords_editor.setPlaceholderText('คอร์ด / chord progression / note'); rl.addWidget(self.chords_editor)
        splitter.addWidget(left); splitter.addWidget(right); splitter.setSizes([900, 500]); root.addWidget(splitter, 1)
        sync_group = QFrame(); sync_layout = QVBoxLayout(sync_group); sync_layout.setContentsMargins(0,0,0,0)
        top = QHBoxLayout(); self.mark_sync_btn = QPushButton('จับเวลาบรรทัดที่เลือก'); self.mark_sync_btn.clicked.connect(self.mark_current_line_sync); self.import_lrc_btn = QPushButton('โหลด .txt / .lrc'); self.import_lrc_btn.clicked.connect(self.load_lyrics_file); self.analyze_chord_btn = QPushButton('Analyze Chord (beta)'); self.analyze_chord_btn.clicked.connect(self.analyze_chord_beta); self.save_lyrics_btn = QPushButton('บันทึก Lyrics / Chords / Sync'); self.save_lyrics_btn.clicked.connect(self.save_current_song)
        for b in [self.mark_sync_btn, self.import_lrc_btn, self.analyze_chord_btn, self.save_lyrics_btn]: top.addWidget(b)
        top.addStretch(); sync_layout.addLayout(top)
        self.sync_list = QListWidget(); sync_layout.addWidget(self.sync_list)
        root.addWidget(sync_group)

    def collect_sync_lines(self):
        lines = []
        for i in range(self.sync_list.count()):
            text = self.sync_list.item(i).text()
            if '  ' in text:
                tm, tx = text.split('  ', 1)
                lines.append({'time': tm.strip(), 'text': tx.strip()})
        return lines

    def current_player_position_label(self):
        if not self.player: return '00:00'
        ms = self.player.position()
        return format_duration(ms / 1000)

    def mark_current_line_sync(self):
        if not self.current_record:
            QMessageBox.information(self, 'แจ้งเตือน', 'กรุณาเลือกเพลงก่อน')
            return
        cursor = self.lyrics_editor.textCursor()
        cursor.select(cursor.LineUnderCursor)
        text = cursor.selectedText().strip()
        if not text:
            QMessageBox.information(self, 'แจ้งเตือน', 'กรุณาวางเคอร์เซอร์ที่บรรทัดเนื้อเพลง')
            return
        self.sync_list.addItem(f"{self.current_player_position_label()}  {text}")
        self.status_label.setText('เพิ่ม sync line แล้ว')

    def load_lyrics_file(self):
        path, _ = QFileDialog.getOpenFileName(self, 'เลือกไฟล์เนื้อเพลง', '', 'Lyrics Files (*.txt *.lrc)')
        if not path: return
        content = open(path, 'r', encoding='utf-8', errors='ignore').read()
        if path.lower().endswith('.lrc'):
            texts = []
            self.sync_list.clear()
            for line in content.splitlines():
                if line.startswith('[') and ']' in line:
                    tag = line[1:line.find(']')]
                    body = line[line.find(']')+1:].strip()
                    texts.append(body)
                    self.sync_list.addItem(f'{tag}  {body}')
                else:
                    texts.append(line)
            self.lyrics_editor.setPlainText('\n'.join(texts))
        else:
            self.lyrics_editor.setPlainText(content)

    def analyze_chord_beta(self):
        if not self.current_record:
            return
        title = (self.current_record.title or self.current_record.file_name).lower()
        key = 'C'
        if 'รัก' in title: key = 'G'
        elif 'ใจ' in title: key = 'D'
        seq = f'Key: {key}\n\nVerse: {key} Am F G\nHook: F G Em Am'
        if not self.chords_editor.toPlainText().strip():
            self.chords_editor.setPlainText(seq)
        self.status_label.setText('วิเคราะห์คอร์ดเบื้องต้นแล้ว (beta)')

    # ---------- Performance ----------
    def build_performance_tab(self):
        root = QHBoxLayout(self.tab_performance)
        left_group = QGroupBox('Live Queue'); left = QVBoxLayout(left_group)
        self.queue_list = QListWidget(); self.queue_list.itemDoubleClicked.connect(self.on_queue_double_clicked); left.addWidget(self.queue_list)
        qbtn = QHBoxLayout(); b1 = QPushButton('ขึ้น'); b2 = QPushButton('ลง'); b3 = QPushButton('ลบจากคิว'); b4 = QPushButton('ล้างคิว'); b1.clicked.connect(self.move_queue_up); b2.clicked.connect(self.move_queue_down); b3.clicked.connect(self.remove_queue_item); b4.clicked.connect(self.clear_queue)
        for b in [b1,b2,b3,b4]: qbtn.addWidget(b)
        left.addLayout(qbtn); root.addWidget(left_group, 3)
        right_group = QGroupBox('Performance View'); right = QVBoxLayout(right_group)
        head = QHBoxLayout(); self.performance_title = QLabel('ยังไม่มีเพลงในคิว'); self.performance_title.setStyleSheet('font-size:26px;font-weight:700;'); self.performance_meta = QLabel('Key: - | BPM: - | Mode: Normal'); head.addWidget(self.performance_title); head.addStretch(); head.addWidget(self.performance_meta); right.addLayout(head)
        self.performance_status = QLabel('Status: Stopped'); self.performance_status.setStyleSheet('font-size:14px;font-weight:600;color:#9fb1c7;')
        right.addWidget(self.performance_status)
        self.performance_progress = QProgressBar(); self.performance_progress.setRange(0, 1000); self.performance_progress.setValue(0); right.addWidget(self.performance_progress)
        self.performance_time = QLabel('00:00 / 00:00'); self.performance_time.setStyleSheet('font-size:13px;color:#b8c6d8;')
        right.addWidget(self.performance_time)
        self.performance_text = QPlainTextEdit(); self.performance_text.setReadOnly(True); self.performance_text.setStyleSheet('font-size:30px; line-height:1.45;'); right.addWidget(self.performance_text, 1)
        tools = QHBoxLayout(); self.perf_prev = QPushButton('⏮'); self.perf_play = QPushButton('▶'); self.perf_stop = QPushButton('⏹'); self.perf_next = QPushButton('⏭'); self.perf_karaoke = QPushButton('🎤 Karaoke OFF'); self.perf_karaoke.setCheckable(True); self.perf_karaoke.clicked.connect(self.toggle_karaoke_mode)
        self.mode_combo = QComboBox(); self.mode_combo.addItems(['เล่นต่ออัตโนมัติ','หยุดเมื่อจบเพลง','Crossfade (อนาคต)'])
        self.transpose_down_btn = QPushButton('Transpose -'); self.transpose_up_btn = QPushButton('Transpose +'); self.transpose_down_btn.clicked.connect(lambda: self.transpose_chords(-1)); self.transpose_up_btn.clicked.connect(lambda: self.transpose_chords(1))
        self.font_minus_btn = QPushButton('A-'); self.font_plus_btn = QPushButton('A+'); self.font_minus_btn.clicked.connect(lambda: self.adjust_performance_font(-2)); self.font_plus_btn.clicked.connect(lambda: self.adjust_performance_font(2))
        self.perf_prev.clicked.connect(self.play_previous_queue); self.perf_play.clicked.connect(self.toggle_performance_play_pause); self.perf_stop.clicked.connect(self.stop_preview); self.perf_next.clicked.connect(self.play_next_queue)
        for w in [self.perf_prev,self.perf_play,self.perf_stop,self.perf_next,self.perf_karaoke,self.mode_combo,self.transpose_down_btn,self.transpose_up_btn,self.font_minus_btn,self.font_plus_btn]: tools.addWidget(w)
        tools.addStretch(); right.addLayout(tools); root.addWidget(right_group, 7)

    def compose_display_text(self, rec: SongRecord) -> str:
        if rec.chords.strip() and rec.lyrics.strip():
            return rec.chords.strip() + '\n\n' + rec.lyrics.strip()
        if rec.chords.strip(): return rec.chords.strip()
        return rec.lyrics.strip() or f'{rec.title or rec.file_name}'

    def add_selected_to_queue(self):
        added = 0
        for rec in self.records:
            if rec.is_selected and rec not in self.performance_queue:
                self.performance_queue.append(rec); added += 1
        self.refresh_queue()
        self.status_label.setText(f'ส่งเข้าคิว Performance แล้ว {added} รายการ')

    def refresh_queue(self):
        self.queue_list.clear()
        for i, rec in enumerate(self.performance_queue, 1):
            item = QListWidgetItem(f'{i:02d}. {rec.title or rec.file_name} - {rec.artist}'.strip(' -'))
            if i-1 == self.current_queue_index:
                item.setBackground(QColor('#4b5c78'))
            self.queue_list.addItem(item)

    def on_queue_double_clicked(self, item):
        self.current_queue_index = self.queue_list.row(item)
        self.play_queue_current()

    def move_queue_up(self):
        idx = self.queue_list.currentRow()
        if idx > 0:
            self.performance_queue[idx-1], self.performance_queue[idx] = self.performance_queue[idx], self.performance_queue[idx-1]
            self.refresh_queue(); self.queue_list.setCurrentRow(idx-1)

    def move_queue_down(self):
        idx = self.queue_list.currentRow()
        if 0 <= idx < len(self.performance_queue)-1:
            self.performance_queue[idx+1], self.performance_queue[idx] = self.performance_queue[idx], self.performance_queue[idx+1]
            self.refresh_queue(); self.queue_list.setCurrentRow(idx+1)

    def remove_queue_item(self):
        idx = self.queue_list.currentRow()
        if idx >= 0:
            del self.performance_queue[idx]
            if self.current_queue_index >= len(self.performance_queue): self.current_queue_index = len(self.performance_queue)-1
            self.refresh_queue()

    def clear_queue(self):
        self.performance_queue = []; self.current_queue_index = -1; self.refresh_queue()

    def toggle_karaoke_mode(self):
        self.karaoke_global_enabled = self.perf_karaoke.isChecked()
        self.current_playback_mode = 'Karaoke' if self.karaoke_global_enabled else 'Normal'
        self.perf_karaoke.setText('🎤 Karaoke ON' if self.karaoke_global_enabled else '🎤 Karaoke OFF')
        if self.current_record:
            self.performance_meta.setText(f'Key: {self.current_record.key_text} | BPM: {self.current_record.bpm_text} | Mode: {self.current_playback_mode}')

    def transpose_chords(self, step: int):
        txt = self.performance_text.toPlainText()
        notes = ['C','C#','D','D#','E','F','F#','G','G#','A','A#','B']
        flats = {'Db':'C#','Eb':'D#','Gb':'F#','Ab':'G#','Bb':'A#'}
        for f,t in flats.items(): txt = txt.replace(f, t)
        def shift(word):
            base = word
            suffix = ''
            if len(base) > 1 and base[1] in ['#','b']:
                note = base[:2]; suffix = base[2:]
            else:
                note = base[:1]; suffix = base[1:]
            if note not in notes: return word
            return notes[(notes.index(note)+step)%12] + suffix
        out = []
        for token in txt.split():
            if token[:1] in 'ABCDEFG': out.append(shift(token))
            else: out.append(token)
        self.performance_text.setPlainText(' '.join(out))

    def adjust_performance_font(self, delta: int):
        f = self.performance_text.font(); size = max(16, min(56, f.pointSize()+delta)); f.setPointSize(size); self.performance_text.setFont(f)

    def play_queue_current(self):
        if self.current_queue_index < 0 and self.performance_queue: self.current_queue_index = 0
        if not (0 <= self.current_queue_index < len(self.performance_queue)): return
        rec = self.performance_queue[self.current_queue_index]
        self.current_record = rec
        self.load_current_record_into_tabs(rec)
        self.play_record(rec)
        self.refresh_queue()

    def play_previous_queue(self):
        if self.current_queue_index > 0: self.current_queue_index -= 1; self.play_queue_current()

    def play_next_queue(self):
        if self.current_queue_index < len(self.performance_queue)-1: self.current_queue_index += 1; self.play_queue_current()
        else: self.stop_preview()

    def toggle_performance_play_pause(self):
        self.toggle_library_play_pause()
        self.perf_play.setText('⏸' if self.player and self.player.state() == QMediaPlayer.PlayingState else '▶')

    # ---------- Karaoke Prep ----------
    def build_karaoke_tab(self):
        root = QVBoxLayout(self.tab_karaoke)
        self.karaoke_out_label = QLabel(f"Output: {self.db.get_setting('karaoke_output', os.path.join(BASE_DIR, 'Karaoke_Outputs'))}")
        root.addWidget(self.karaoke_out_label)
        top = QHBoxLayout(); add_files = QPushButton('เพิ่มเพลง'); add_folder = QPushButton('เพิ่มทั้งโฟลเดอร์'); remove = QPushButton('ลบที่เลือก'); clear = QPushButton('ล้างรายการ'); start = QPushButton('สร้าง Instrumental')
        add_files.clicked.connect(self.add_files_to_karaoke); add_folder.clicked.connect(self.add_folder_to_karaoke); remove.clicked.connect(self.remove_selected_karaoke_rows); clear.clicked.connect(self.clear_karaoke_rows); start.clicked.connect(self.start_karaoke_build)
        for b in [add_files, add_folder, remove, clear, start]: top.addWidget(b)
        top.addStretch(); root.addLayout(top)
        self.karaoke_table = QTableWidget(0, 4); self.karaoke_table.setHorizontalHeaderLabels(['ไฟล์เพลง','สถานะ','Instrumental','Vocal']); self.karaoke_table.setSelectionBehavior(QAbstractItemView.SelectRows); self.karaoke_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch); self.karaoke_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents); self.karaoke_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch); self.karaoke_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch); root.addWidget(self.karaoke_table, 1)
        self.karaoke_progress = QProgressBar(); root.addWidget(self.karaoke_progress)
        self.karaoke_hint = QLabel('หมายเหตุ: ระบบจะตรวจ demucs อัตโนมัติ และบันทึก instrumental / vocal กลับเข้าไลบรารี'); self.karaoke_hint.setObjectName('topInfoLabel'); root.addWidget(self.karaoke_hint)

    def add_files_to_karaoke(self):
        files, _ = QFileDialog.getOpenFileNames(self, 'เลือกเพลง', '', 'Audio Files (*.mp3 *.wav *.flac *.m4a *.aac *.ogg *.wma)')
        for path in files: self.append_karaoke_row(path)

    def add_folder_to_karaoke(self):
        folder = QFileDialog.getExistingDirectory(self, 'เลือกโฟลเดอร์')
        if not folder: return
        for root, _, names in os.walk(folder):
            for n in names:
                if n.lower().endswith(AUDIO_EXTS): self.append_karaoke_row(os.path.join(root, n))

    def append_karaoke_row(self, path: str):
        for row in range(self.karaoke_table.rowCount()):
            if self.karaoke_table.item(row, 0).text() == path: return
        row = self.karaoke_table.rowCount(); self.karaoke_table.insertRow(row)
        for col, val in enumerate([path, 'ยังไม่สร้าง', '', '']): self.karaoke_table.setItem(row, col, QTableWidgetItem(val))

    def remove_selected_karaoke_rows(self):
        rows = sorted({idx.row() for idx in self.karaoke_table.selectionModel().selectedRows()}, reverse=True)
        for row in rows: self.karaoke_table.removeRow(row)

    def clear_karaoke_rows(self):
        self.karaoke_table.setRowCount(0)

    def demucs_available(self) -> bool:
        try:
            proc = subprocess.run([sys.executable, '-m', 'demucs', '--help'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15)
            return proc.returncode == 0 or 'usage' in (proc.stdout + proc.stderr).lower()
        except Exception:
            return False

    def start_karaoke_build(self):
        if self.karaoke_table.rowCount() == 0:
            QMessageBox.information(self, 'แจ้งเตือน', 'ยังไม่มีรายการ')
            return
        if not self.demucs_available():
            QMessageBox.warning(self, 'Karaoke Prep', 'ยังไม่พบ demucs ในระบบ\nติดตั้งก่อนด้วยคำสั่ง: pip install demucs')
            return
        out = self.db.get_setting('karaoke_output', os.path.join(BASE_DIR, 'Karaoke_Outputs'))
        rows = []
        for row in range(self.karaoke_table.rowCount()):
            rows.append((row, self.karaoke_table.item(row, 0).text()))
            self.karaoke_table.item(row, 1).setText('รอประมวลผล')
        self.kworker = DemucsWorker(rows, out)
        self.kworker.progress.connect(self.karaoke_progress.setValue)
        self.kworker.one_done.connect(self.on_one_karaoke_done)
        self.kworker.finished_all.connect(lambda: self.status_label.setText('สร้าง Karaoke เสร็จแล้ว'))
        self.kworker.start()

    def on_one_karaoke_done(self, row: int, status: str, inst: str, vocal: str):
        self.karaoke_table.item(row, 1).setText(status)
        self.karaoke_table.item(row, 2).setText(inst)
        self.karaoke_table.item(row, 3).setText(vocal)
        path = self.karaoke_table.item(row, 0).text()
        for rec in self.records:
            if rec.full_path == path:
                rec.instrumental_path = inst; rec.vocal_path = vocal; self.db.save_song(rec); break

    # ---------- Reports ----------
    def build_reports_tab(self):
        root = QVBoxLayout(self.tab_reports)
        self.report_text = QTextEdit(); self.report_text.setReadOnly(True); root.addWidget(self.report_text)

    def update_reports(self):
        counts = Counter(r.auto_category for r in self.records)
        lines = [self.summary_files.text(), self.summary_found.text(), self.summary_groups.text(), self.summary_size.text(), '', 'สรุปตามหมวด:']
        for k in CATEGORIES[1:]:
            if k in counts:
                lines.append(f'- {k}: {counts[k]}')
        lines.append('')
        lines.append('รายการที่ควรตรวจสอบ:')
        for r in self.records:
            if r.auto_category in {'เพลงซ้ำ/ควรตรวจสอบ', 'ชื่อไฟล์มีปัญหา', 'ข้อมูลขัดแย้ง', 'ไฟล์เสียจริง', 'เพลงความยาวผิดปกติ'}:
                lines.append(f'- {r.file_name} | {r.auto_category} | {r.remark}')
        self.report_text.setPlainText('\n'.join(lines))

    # ---------- Settings ----------
    def build_settings_tab(self):
        root = QVBoxLayout(self.tab_settings)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(10)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        root.addWidget(scroll)

        content = QWidget()
        scroll.setWidget(content)
        body = QVBoxLayout(content)
        body.setSpacing(12)

        hero = QFrame(); hero.setObjectName('panelCard')
        hero_layout = QHBoxLayout(hero)
        hero_layout.setContentsMargins(18, 18, 18, 18)
        logo = QLabel()
        logo.setPixmap(get_program_icon_pixmap(120))
        hero_layout.addWidget(logo, 0, Qt.AlignTop)
        info_wrap = QVBoxLayout()
        self.settings_title = QLabel(f'{APP_TITLE} {APP_VERSION}')
        self.settings_title.setStyleSheet('font-size:26px;font-weight:700;')
        self.settings_desc = QLabel('โปรแกรมสำหรับคัดกรองไฟล์เพลง ซ่อม workflow การจัดเก็บ เตรียมคิวแสดงสด และทำ Karaoke Prep ในที่เดียว')
        self.settings_desc.setWordWrap(True)
        self.settings_desc.setStyleSheet('font-size:14px;color:#c8d6e7;')
        self.settings_credit = QLabel('พัฒนาโดย Suraphun Inopas')
        self.settings_credit.setStyleSheet('font-size:16px;font-weight:700;')
        info_wrap.addWidget(self.settings_title)
        info_wrap.addWidget(self.settings_desc)
        info_wrap.addWidget(self.settings_credit)
        info_wrap.addStretch()
        hero_layout.addLayout(info_wrap, 1)
        body.addWidget(hero)

        grid = QGridLayout()
        grid.setHorizontalSpacing(12)
        grid.setVerticalSpacing(12)
        body.addLayout(grid)

        about_box = QGroupBox('About / คุณสมบัติหลัก')
        about_layout = QVBoxLayout(about_box)
        self.about_features = QLabel('• ตรวจไฟล์เสีย, header พัง, bitrate เพี้ยน\n• คัดแยกหมวดเพลงอัตโนมัติ\n• เปลี่ยนชื่อไฟล์จาก metadata หรือแก้เองทีละไฟล์\n• จัดคิว Performance และ Karaoke Prep\n• รองรับภาษาไทย / English')
        self.about_features.setWordWrap(True)
        about_layout.addWidget(self.about_features)
        grid.addWidget(about_box, 0, 0)

        app_box = QGroupBox('Application')
        app_form = QFormLayout(app_box)
        self.lang_combo = QComboBox()
        for code, label in LANGUAGE_LABELS.items():
            self.lang_combo.addItem(label, code)
        self.lang_combo.setCurrentIndex(max(0, self.lang_combo.findData(self.current_language)))
        self.lang_combo.currentIndexChanged.connect(self.on_language_changed)
        self.detection_toggle = QCheckBox('เปิดใช้ระบบตรวจจับไฟล์อัตโนมัติ')
        self.detection_toggle.setChecked(self.file_detection_enabled)
        self.detection_toggle.stateChanged.connect(self.on_detection_toggle_changed)
        self.external_player_label = QLabel('ffplay: พร้อมใช้งาน' if self.can_use_ffplay() else 'ffplay: ยังไม่พบใน PATH')
        app_form.addRow('Language', self.lang_combo)
        app_form.addRow('Detection', self.detection_toggle)
        app_form.addRow('External Player', self.external_player_label)
        grid.addWidget(app_box, 0, 1)

        playback_box = QGroupBox('Playback & Karaoke')
        playback_form = QFormLayout(playback_box)
        self.setting_output = QLineEdit(self.db.get_setting('karaoke_output', os.path.join(BASE_DIR, 'Karaoke_Outputs')))
        pick = QPushButton('เลือก Output Folder')
        pick.clicked.connect(self.pick_karaoke_output)
        outwrap = QHBoxLayout(); outwrap.addWidget(self.setting_output); outwrap.addWidget(pick); outw = QWidget(); outw.setLayout(outwrap)
        self.setting_font = QSpinBox(); self.setting_font.setRange(18, 56); self.setting_font.setValue(30); self.setting_font.valueChanged.connect(lambda v: self.performance_text.setStyleSheet(f'font-size:{v}px; line-height:1.45;'))
        self.fallback_combo = QComboBox(); self.fallback_combo.addItems(['ถ้าไม่มี instrumental ให้เล่นไฟล์ปกติ','ถ้าไม่มี instrumental ให้แจ้งเตือนแล้วไม่เล่น'])
        self.vocal_slider = QSlider(Qt.Horizontal); self.vocal_slider.setRange(-12, 12); self.vocal_slider.setValue(0)
        self.guitar_slider = QSlider(Qt.Horizontal); self.guitar_slider.setRange(-12, 12); self.guitar_slider.setValue(0)
        playback_form.addRow('Karaoke output', outw)
        playback_form.addRow('Performance font', self.setting_font)
        playback_form.addRow('Karaoke behavior', self.fallback_combo)
        playback_form.addRow('Vocal level (prep)', self.vocal_slider)
        playback_form.addRow('Guitar level (prep)', self.guitar_slider)
        grid.addWidget(playback_box, 1, 0, 1, 2)

        notes = QGroupBox('Notes')
        notes_layout = QVBoxLayout(notes)
        self.settings_note = QLabel('แนะนำ: ติดตั้ง FFmpeg เพื่อให้ ffplay fallback ช่วยเปิดไฟล์ที่ QMediaPlayer เล่นไม่ได้ และใช้หน้า Library เพื่อตรวจไฟล์ผิดปกติก่อนนำเข้าระบบออกอากาศ')
        self.settings_note.setWordWrap(True)
        notes_layout.addWidget(self.settings_note)
        body.addWidget(notes)
        body.addStretch()

    def pick_karaoke_output(self):
        folder = QFileDialog.getExistingDirectory(self, 'เลือกโฟลเดอร์ output')
        if folder:
            self.setting_output.setText(folder)
            self.db.set_setting('karaoke_output', folder)
            self.karaoke_out_label.setText(f'Output: {folder}')

    def on_language_changed(self):
        code = self.lang_combo.currentData() or 'th'
        self.current_language = code
        self.db.set_setting('ui_language', code)
        self.apply_language()

    def on_detection_toggle_changed(self):
        self.file_detection_enabled = self.detection_toggle.isChecked()
        self.db.set_setting('file_detection_enabled', '1' if self.file_detection_enabled else '0')

    def apply_language(self):
        labels = UI_TEXT.get(self.current_language, UI_TEXT['th'])
        self.tabs.setTabText(0, labels['library'])
        self.tabs.setTabText(1, labels['song_detail'])
        self.tabs.setTabText(2, labels['lyrics'])
        self.tabs.setTabText(3, labels['performance'])
        self.tabs.setTabText(4, labels['karaoke'])
        self.tabs.setTabText(5, labels['reports'])
        self.tabs.setTabText(6, labels['settings'])
        if hasattr(self, 'library_title'):
            self.library_title.setText('Library')
        if hasattr(self, 'library_subtitle'):
            self.library_subtitle.setText('Audio Scan • Duplicate • Metadata • Playlist • Performance')
        if hasattr(self, 'browse_btn'):
            self.browse_btn.setText('Browse Folder' if self.current_language == 'en' else 'เลือกโฟลเดอร์')
            self.scan_analyze_btn.setText('Scan & Analyze' if self.current_language == 'en' else 'สแกนและวิเคราะห์')
            self.show_found_only.setText('Show issues only' if self.current_language == 'en' else 'แสดงเฉพาะผลที่ควรตรวจสอบ')
            self.show_selected_only.setText('Show selected only' if self.current_language == 'en' else 'แสดงเฉพาะไฟล์ที่เลือก')
            self.clear_btn.setText('Clear Data' if self.current_language == 'en' else 'ล้างข้อมูล')
            self.search_box.setPlaceholderText('Search title / artist / path' if self.current_language == 'en' else 'ค้นหาชื่อเพลง / ศิลปิน / path')
            self.folder_input.setPlaceholderText('Choose music folder...' if self.current_language == 'en' else 'เลือกโฟลเดอร์คลังเพลง...')
        if hasattr(self, 'settings_desc'):
            if self.current_language == 'en':
                self.settings_desc.setText('A desktop application for music library inspection, filename cleanup, performance queue management, and karaoke preparation.')
                self.settings_credit.setText('Created by Suraphun Inopas')
                self.about_features.setText('• Detect broken headers, invalid bitrate, and corrupted audio\n• Auto-categorize library issues\n• Rename files from metadata or manually one-by-one\n• Manage Performance queue and Karaoke Prep\n• Thai / English interface')
                self.settings_note.setText('Tip: install FFmpeg so ffplay fallback can open files that QMediaPlayer cannot decode reliably.')
                self.detection_toggle.setText('Enable automatic file issue detection')
            else:
                self.settings_desc.setText('โปรแกรมสำหรับคัดกรองไฟล์เพลง ซ่อม workflow การจัดเก็บ เตรียมคิวแสดงสด และทำ Karaoke Prep ในที่เดียว')
                self.settings_credit.setText('พัฒนาโดย Suraphun Inopas')
                self.about_features.setText('• ตรวจไฟล์เสีย, header พัง, bitrate เพี้ยน\n• คัดแยกหมวดเพลงอัตโนมัติ\n• เปลี่ยนชื่อไฟล์จาก metadata หรือแก้เองทีละไฟล์\n• จัดคิว Performance และ Karaoke Prep\n• รองรับภาษาไทย / English')
                self.settings_note.setText('แนะนำ: ติดตั้ง FFmpeg เพื่อให้ ffplay fallback ช่วยเปิดไฟล์ที่ QMediaPlayer เล่นไม่ได้ และใช้หน้า Library เพื่อตรวจไฟล์ผิดปกติก่อนนำเข้าระบบออกอากาศ')
                self.detection_toggle.setText('เปิดใช้ระบบตรวจจับไฟล์อัตโนมัติ')
        if hasattr(self, 'external_player_label'):
            if self.current_language == 'en':
                self.external_player_label.setText('ffplay: available' if self.can_use_ffplay() else 'ffplay: not found in PATH')
            else:
                self.external_player_label.setText('ffplay: พร้อมใช้งาน' if self.can_use_ffplay() else 'ffplay: ยังไม่พบใน PATH')

    def update_performance_status(self, state_text: str = ''):
        if not hasattr(self, 'performance_status'):
            return
        title = self.current_record.title if self.current_record else ''
        name = title or (self.current_record.file_name if self.current_record else '-')
        pos_text = self.library_position_label.text() if hasattr(self, 'library_position_label') else '00:00'
        dur_text = self.library_duration_label.text() if hasattr(self, 'library_duration_label') else '00:00'
        mismatch = ' | Duration mismatch' if self.duration_mismatch_flag else ''
        self.performance_status.setText(f'Status: {state_text or "Stopped"} | {name}{mismatch}')
        self.performance_time.setText(f'{pos_text} / {dur_text}')

    def detect_runtime_mismatch(self, rec: Optional[SongRecord], actual_ms: int):
        self.duration_mismatch_flag = False
        if not rec or actual_ms <= 0:
            return
        expected = int((rec.duration_seconds or 0) * 1000)
        if expected <= 0:
            return
        ratio = actual_ms / max(expected, 1)
        if ratio < 0.55 or ratio > 1.80:
            self.duration_mismatch_flag = True
            if 'เวลาเพี้ยน' not in (rec.issue_flags or ''):
                rec.issue_flags = '|'.join(filter(None, [rec.issue_flags, 'เวลาเพี้ยน']))
            if 'เวลาเพี้ยน' not in (rec.remark or ''):
                extra = f'เวลาไฟล์ไม่ตรงจริง ({format_duration(expected/1000)} vs {format_duration(actual_ms/1000)})'
                rec.remark = extra if rec.remark in ('', '-') else rec.remark + ' | ' + extra
            if rec.status != 'BROKEN' and rec.auto_category == 'เพลงมาตรฐาน':
                rec.auto_category = 'เพลงความยาวผิดปกติ'

    # ---------- Player ----------
    def stop_ffplay(self):
        if self.ffplay_proc and self.ffplay_proc.poll() is None:
            try:
                self.ffplay_proc.kill()
            except Exception:
                pass
        self.ffplay_proc = None

    def can_use_ffplay(self) -> bool:
        return is_ffplay_available()

    def play_with_ffplay(self, path: str):
        self.stop_ffplay()
        self.ffplay_proc = subprocess.Popen(['ffplay', '-nodisp', '-autoexit', '-loglevel', 'quiet', path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.update_performance_status('Playing (ffplay)')

    def on_qt_player_error(self, *args):
        if not self.current_play_path:
            return
        self.prefer_ffplay_paths.add(self.current_play_path)
        if self.can_use_ffplay():
            self.play_with_ffplay(self.current_play_path)
            self.now_playing_label.setText(os.path.basename(self.current_play_path))
            self.library_play_pause_btn.setText('⏸')
            self.perf_play.setText('⏸')
        else:
            self.status_label.setText('QMediaPlayer เล่นไฟล์นี้ไม่ได้ และยังไม่พบ ffplay')

    def play_record(self, rec: SongRecord):
        path = rec.full_path
        if self.karaoke_global_enabled:
            if rec.instrumental_path and os.path.exists(rec.instrumental_path):
                path = rec.instrumental_path
            elif self.fallback_combo.currentIndex() == 1:
                QMessageBox.information(self, 'Karaoke Mode', 'ยังไม่มีไฟล์ Instrumental สำหรับเพลงนี้')
                return
        if not os.path.exists(path):
            QMessageBox.warning(self, 'แจ้งเตือน', f'ไม่พบไฟล์\n{path}')
            return
        self.current_play_path = path
        self.measured_duration_ms = 0
        self.reported_duration_ms = 0
        self.stop_ffplay()
        prefer_external = path in self.prefer_ffplay_paths or '[]' in os.path.basename(path) or rec.status == 'BROKEN'
        if prefer_external and self.can_use_ffplay():
            self.play_with_ffplay(path)
        elif self.player:
            self.player.stop()
            self.player.setMedia(QMediaContent(QUrl.fromLocalFile(path)))
            self.player.play()
        elif self.can_use_ffplay():
            self.play_with_ffplay(path)
        else:
            QMessageBox.information(self, 'แจ้งเตือน', 'เครื่องนี้ยังไม่รองรับ audio playback')
            return
        self.library_play_pause_btn.setText('⏸')
        self.perf_play.setText('⏸')
        self.now_playing_label.setText(rec.file_name)

    def get_selected_record(self) -> Optional[SongRecord]:
        row = self.table.currentRow()
        if 0 <= row < len(self.records):
            return self.records[row]
        return self.current_record

    def play_selected(self):
        rec = self.get_selected_record()
        if not rec:
            QMessageBox.information(self, 'แจ้งเตือน', 'กรุณาเลือกเพลงก่อน')
            return
        self.current_record = rec
        self.load_current_record_into_tabs(rec)
        self.play_record(rec)

    def toggle_library_play_pause(self):
        if self.ffplay_proc and self.ffplay_proc.poll() is None:
            self.stop_ffplay()
            self.library_play_pause_btn.setText('▶'); self.perf_play.setText('▶'); self.update_performance_status('Stopped')
            return
        if not self.player:
            self.play_selected()
            return
        state = self.player.state()
        if state == QMediaPlayer.PlayingState:
            self.player.pause(); self.library_play_pause_btn.setText('▶'); self.perf_play.setText('▶')
        elif state == QMediaPlayer.PausedState:
            self.player.play(); self.library_play_pause_btn.setText('⏸'); self.perf_play.setText('⏸')
        else:
            self.play_selected()

    def play_previous_in_table(self):
        row = self.table.currentRow()
        if row > 0:
            self.table.selectRow(row-1); self.play_selected()

    def play_next_in_table(self):
        row = self.table.currentRow()
        if row < self.table.rowCount()-1:
            self.table.selectRow(row+1); self.play_selected()

    def stop_preview(self):
        self.stop_ffplay()
        if self.player:
            self.player.stop(); self.player.setPosition(0)
        self.library_seek_slider.setValue(0); self.seek_reset_labels(); self.library_play_pause_btn.setText('▶'); self.perf_play.setText('▶'); self.performance_progress.setValue(0); self.update_performance_status('Stopped')

    def seek_reset_labels(self):
        self.library_position_label.setText('00:00'); self.library_duration_label.setText('00:00')

    def on_slider_pressed(self):
        self.is_slider_dragging = True

    def on_library_slider_released(self):
        self.is_slider_dragging = False
        if self.player: self.player.setPosition(self.library_seek_slider.value())

    def on_library_slider_moved(self, value: int):
        self.library_position_label.setText(format_duration(value / 1000))

    def on_player_position_changed(self, pos: int):
        if not self.is_slider_dragging: self.library_seek_slider.setValue(pos)
        self.library_position_label.setText(format_duration(pos / 1000))
        maxv = max(self.library_seek_slider.maximum(), 1)
        self.performance_progress.setValue(int((pos / maxv) * 1000))
        self.measured_duration_ms = max(self.measured_duration_ms, pos)
        self.highlight_sync_line(pos)
        self.update_performance_status('Playing' if self.player and self.player.state() == QMediaPlayer.PlayingState else 'Paused')

    def on_player_duration_changed(self, dur: int):
        self.reported_duration_ms = dur
        self.library_seek_slider.setRange(0, dur); self.library_duration_label.setText(format_duration(dur / 1000))
        self.detect_runtime_mismatch(self.current_record, dur)
        self.update_performance_status('Loaded')

    def on_player_state_changed(self, state):
        if not self.player:
            return
        if state == QMediaPlayer.PlayingState:
            self.library_play_pause_btn.setText('⏸'); self.perf_play.setText('⏸'); self.update_performance_status('Playing')
        elif state == QMediaPlayer.PausedState:
            self.library_play_pause_btn.setText('▶'); self.perf_play.setText('▶'); self.update_performance_status('Paused')
        else:
            self.library_play_pause_btn.setText('▶'); self.perf_play.setText('▶'); self.update_performance_status('Stopped')

    def on_media_status_changed(self, status):
        if not self.player: return
        if status == QMediaPlayer.EndOfMedia:
            if self.measured_duration_ms > 0:
                self.detect_runtime_mismatch(self.current_record, self.measured_duration_ms)
            mode = self.mode_combo.currentText()
            if mode == 'เล่นต่ออัตโนมัติ': self.play_next_queue()
            elif mode == 'หยุดเมื่อจบเพลง': self.stop_preview()
            else: self.play_next_queue()

    def toggle_mute(self):
        self.mute_state = not self.mute_state
        if self.player: self.player.setMuted(self.mute_state)
        self.library_mute_btn.setText('🔇' if self.mute_state else '🔊')

    def on_volume_changed(self, value: int):
        self.current_volume = value
        self.cfg['volume'] = value
        save_config(self.cfg)
        if self.player: self.player.setVolume(value)

    def highlight_sync_line(self, pos_ms: int):
        if not self.current_record: return
        try:
            lines = json.loads(self.current_record.sync_lines or '[]')
            current_idx = -1
            sec = pos_ms / 1000
            for idx, obj in enumerate(lines):
                t = obj.get('time', '00:00')
                parts = t.split(':')
                total = int(parts[0]) * 60 + float(parts[1])
                if sec >= total: current_idx = idx
            if current_idx >= 0 and current_idx < self.sync_list.count():
                self.sync_list.setCurrentRow(current_idx)
        except Exception:
            pass


def main():
    app = QApplication(sys.argv)
    app.setApplicationName(APP_TITLE)
    app.setWindowIcon(get_app_icon())
    splash = get_splash_pixmap()
    if not splash.isNull():
        from PyQt5.QtWidgets import QSplashScreen
        sp = QSplashScreen(splash)
        sp.show()
        sp.showMessage('Loading interface...\nPreparing music library...\nInitializing performance tools...', Qt.AlignLeft | Qt.AlignTop, QColor('#eef3fa'))
        app.processEvents(); time.sleep(1.5)
    else:
        sp = None
    w = MainWindow(); w.show()
    if sp: sp.finish(w)
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
