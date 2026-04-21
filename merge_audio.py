#!/usr/bin/env python3
"""
Batch merge external audio tracks into video files using ffmpeg.

Pairs video + audio files in a directory and muxes them without re-encoding.
First tries exact base-name matching, then falls back to matching by
extracted episode number (S01E01, E01, " - 01", [01], etc).

Usage:
    python3 merge_audio.py                          # opens folder picker (GUI)
    python3 merge_audio.py /path/to/folder
    python3 merge_audio.py /path/to/folder -j 4     # 4 parallel jobs
    python3 merge_audio.py /path/to/folder --dry-run
"""
import argparse
import collections
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

VIDEO_EXTS = {'.mkv', '.mp4', '.avi', '.mov', '.webm', '.flv', '.m4v', '.ts'}
AUDIO_EXTS = {'.mp3', '.aac', '.m4a', '.mka', '.opus', '.flac', '.wav', '.ogg', '.ac3'}

NOISE_RE = re.compile(
    r'\b('
    r'\d{3,4}[pi]|'
    r'[xh]\.?26[45]|hevc|avc|av1|'
    r'web-?dl|web-?rip|bluray|blu-ray|bdrip|dvdrip|hdtv|hdrip|'
    r'aac|ac3|flac|dts-?hd|dts|opus|mp3|eac3|'
    r'[257]\.1|stereo|'
    r'10bit|8bit|hi10p|hdr|sdr|'
    r'multi|dual|dub|sub|subs'
    r')\b',
    re.IGNORECASE,
)


# -------- filename matching --------

def extract_episode(stem):
    cleaned = NOISE_RE.sub(' ', stem)

    m = re.search(r's(\d{1,2})\s*e(\d{1,3})', cleaned, re.IGNORECASE)
    if m:
        return f's{int(m.group(1)):02d}e{int(m.group(2)):02d}'
    m = re.search(r'\bep?\.?\s*(\d{1,3})\b', cleaned, re.IGNORECASE)
    if m:
        return f'e{int(m.group(1)):02d}'
    m = re.search(r'\s-\s*(\d{1,3})(?:\s|$|v\d)', cleaned)
    if m:
        return f'e{int(m.group(1)):02d}'
    m = re.search(r'\[(\d{1,3})\]', cleaned)
    if m:
        return f'e{int(m.group(1)):02d}'
    nums = [int(n) for n in re.findall(r'\b(\d{1,3})\b', cleaned)]
    candidates = [n for n in nums if 1 <= n <= 200]
    if candidates:
        return f'e{candidates[-1]:02d}'
    return None


def collect_files(directory):
    videos, audios = [], []
    for f in sorted(directory.iterdir()):
        if not f.is_file():
            continue
        ext = f.suffix.lower()
        if ext in VIDEO_EXTS:
            videos.append(f)
        elif ext in AUDIO_EXTS:
            audios.append(f)
    return videos, audios


def find_pairs(videos, audios):
    v_by_stem = {v.stem: v for v in videos}
    a_by_stem = {a.stem: a for a in audios}
    pairs = [(v_by_stem[s], a_by_stem[s]) for s in v_by_stem if s in a_by_stem]
    if pairs:
        return pairs, 'exact filename', []

    v_by_ep, a_by_ep = {}, {}
    for v in videos:
        ep = extract_episode(v.stem)
        if ep:
            v_by_ep.setdefault(ep, []).append(v)
    for a in audios:
        ep = extract_episode(a.stem)
        if ep:
            a_by_ep.setdefault(ep, []).append(a)

    pairs, ambiguous = [], []
    for ep in sorted(v_by_ep):
        if ep not in a_by_ep:
            continue
        if len(v_by_ep[ep]) == 1 and len(a_by_ep[ep]) == 1:
            pairs.append((v_by_ep[ep][0], a_by_ep[ep][0]))
        else:
            ambiguous.append(ep)
    return pairs, 'episode number', ambiguous


# -------- ffmpeg helpers --------

def get_duration(path):
    if not shutil.which('ffprobe'):
        return 0.0
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error',
             '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', str(path)],
            capture_output=True, text=True, timeout=10,
        )
        return float(result.stdout.strip())
    except (ValueError, subprocess.SubprocessError):
        return 0.0


def parse_ffmpeg_time(value):
    try:
        if not value or value.startswith('N/A'):
            return 0.0
        h, m, s = value.split(':')
        return int(h) * 3600 + int(m) * 60 + float(s)
    except (ValueError, AttributeError):
        return 0.0


def merge(video, audio, output, keep_original_audio, audio_codec, progress_cb=None):
    """Mux video+audio via ffmpeg. Returns (success, error_text_or_None)."""
    duration = get_duration(video)

    cmd = [
        'ffmpeg', '-y', '-nostats',
        '-i', str(video),
        '-i', str(audio),
        '-c:v', 'copy',
        '-c:a', audio_codec,
        '-map', '0:v',
    ]
    if keep_original_audio:
        cmd += ['-map', '0:a?']
    cmd += ['-map', '1:a', '-progress', 'pipe:1', str(output)]

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1, errors='replace',
    )

    stderr_tail = collections.deque(maxlen=40)

    def drain():
        for line in proc.stderr:
            stderr_tail.append(line)

    t = threading.Thread(target=drain, daemon=True)
    t.start()

    start = time.monotonic()
    last_drawn = 0.0
    current = 0.0

    try:
        for line in proc.stdout:
            line = line.strip()
            if '=' not in line:
                continue
            key, _, value = line.partition('=')
            if key == 'out_time':
                current = parse_ffmpeg_time(value)
            elif key == 'progress':
                now = time.monotonic()
                if progress_cb and (value == 'end' or now - last_drawn >= 1.0):
                    progress_cb(current, duration, now - start)
                    last_drawn = now
                if value == 'end':
                    break
    except KeyboardInterrupt:
        proc.terminate()
        proc.wait()
        raise

    proc.wait()
    t.join(timeout=1)

    if progress_cb and proc.returncode == 0 and duration > 0:
        progress_cb(duration, duration, time.monotonic() - start)

    if proc.returncode != 0:
        return False, ''.join(stderr_tail)[-800:]
    return True, None


# -------- display --------

def _enable_windows_vt():
    """Enable ANSI escape sequence processing on Windows 10+."""
    if sys.platform != 'win32':
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_ulong()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return
        kernel32.SetConsoleMode(handle, mode.value | 4)  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
    except Exception:
        pass


def _term_width():
    try:
        return max(40, shutil.get_terminal_size((100, 20)).columns)
    except Exception:
        return 100


def _truncate(s, width):
    return s if len(s) <= width else s[:width]


def _bar(current, total, width=20):
    if total <= 0:
        return '-' * width
    pct = min(max(current / total, 0.0), 1.0)
    filled = int(pct * width)
    return '#' * filled + '-' * (width - filled)


def draw_progress_inline(current, total, elapsed, width=30):
    """Single-line in-place progress for serial mode."""
    if total > 0:
        pct = min(current / total, 1.0) * 100
        speed = current / elapsed if elapsed > 0 else 0.0
        eta = int((total - current) / speed) if speed > 0 else 0
        line = f'  [{_bar(current, total, width)}] {pct:5.1f}%  {speed:4.1f}x  eta {eta:3d}s'
    else:
        pos = int(elapsed * 2) % (width * 2)
        if pos >= width:
            pos = 2 * width - pos - 1
        pos = max(0, min(pos, width - 1))
        bar = '-' * pos + '#' + '-' * (width - pos - 1)
        line = f'  [{bar}]  {elapsed:5.1f}s elapsed'
    sys.stdout.write('\r' + line + '    ')
    sys.stdout.flush()


class ParallelDisplay:
    """Thread-safe multi-line progress display kept at bottom of terminal.

    Layout:
        <log history scrolls above>
        [W1] ...
        [W2] ...
        ...
        <blank line>
        Overall: X/Y done ...
    """

    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.slots = [f'[W{i + 1}] idle' for i in range(num_workers)]
        self.overall = ''
        self.lock = threading.Lock()
        _enable_windows_vt()
        # Reserve num_workers + 2 lines
        sys.stdout.write('\n' * (num_workers + 2))
        sys.stdout.flush()

    def _redraw(self):
        total = self.num_workers + 2
        width = _term_width() - 1
        sys.stdout.write(f'\033[{total}F')  # up N lines, column 0
        for s in self.slots:
            sys.stdout.write(f'\033[2K{_truncate(s, width)}\n')
        sys.stdout.write('\033[2K\n')
        sys.stdout.write(f'\033[2K{_truncate(self.overall, width)}\n')
        sys.stdout.flush()

    def set_slot(self, idx, text):
        with self.lock:
            self.slots[idx] = text
            self._redraw()

    def set_overall(self, text):
        with self.lock:
            self.overall = text
            self._redraw()

    def log(self, message):
        """Insert a permanent log line above the progress display."""
        with self.lock:
            total = self.num_workers + 2
            width = _term_width() - 1
            sys.stdout.write(f'\033[{total}F')
            sys.stdout.write(f'\033[2K{_truncate(message, width)}\n')
            for s in self.slots:
                sys.stdout.write(f'\033[2K{_truncate(s, width)}\n')
            sys.stdout.write('\033[2K\n')
            sys.stdout.write(f'\033[2K{_truncate(self.overall, width)}\n')
            sys.stdout.flush()


# -------- processing --------

def process_serial(pairs, args, output_dir):
    ok = fail = 0
    for i, (video, audio) in enumerate(pairs, 1):
        out = output_dir / f'{video.stem}.{args.ext.lstrip(".")}'
        print(f'[{i}/{len(pairs)}] {out.name}')
        success, err = merge(
            video, audio, out,
            args.keep_original, args.audio_codec,
            progress_cb=draw_progress_inline,
        )
        sys.stdout.write('\n')
        if success:
            print('  -> OK')
            ok += 1
        else:
            print('  -> FAILED')
            if err:
                print(f'  ffmpeg error:\n{err}', file=sys.stderr)
            fail += 1
    return ok, fail


def process_parallel(pairs, args, output_dir, num_workers):
    total = len(pairs)
    display = ParallelDisplay(num_workers)
    display.log(f'Parallel mode: {num_workers} workers, {total} files queued.')

    queue = collections.deque(enumerate(pairs, 1))
    lock = threading.Lock()
    stats = {'ok': 0, 'fail': 0, 'active': 0}

    def update_overall():
        done = stats['ok'] + stats['fail']
        remaining = total - done - stats['active']
        display.set_overall(
            f'Overall: {done}/{total} done  |  '
            f'{stats["active"]} running  |  {remaining} queued  |  '
            f'{stats["fail"]} failed'
        )

    def worker(slot):
        while True:
            with lock:
                if not queue:
                    stats_active_now = stats['active']
                    break_out = True
                else:
                    i, (video, audio) = queue.popleft()
                    stats['active'] += 1
                    break_out = False
            if break_out:
                display.set_slot(slot, f'[W{slot + 1}] idle')
                return

            update_overall()
            out = output_dir / f'{video.stem}.{args.ext.lstrip(".")}'
            display.log(f'[{i}/{total}] START  {video.name}')
            display.set_slot(slot, f'[W{slot + 1}] [{i}/{total}] starting  {video.name}')

            def cb(current, duration, elapsed, _i=i, _name=video.name, _slot=slot):
                if duration > 0:
                    pct = min(current / duration, 1.0) * 100
                    speed = current / elapsed if elapsed > 0 else 0.0
                    eta = int((duration - current) / speed) if speed > 0 else 0
                    text = (f'[W{_slot + 1}] [{_i}/{total}] '
                            f'[{_bar(current, duration, 20)}] {pct:5.1f}%  '
                            f'{speed:4.1f}x  eta {eta:3d}s  {_name}')
                else:
                    text = (f'[W{_slot + 1}] [{_i}/{total}] '
                            f'{elapsed:5.1f}s elapsed  {_name}')
                display.set_slot(_slot, text)

            t_start = time.monotonic()
            success, err = merge(
                video, audio, out,
                args.keep_original, args.audio_codec,
                progress_cb=cb,
            )
            dt = time.monotonic() - t_start

            with lock:
                stats['active'] -= 1
                if success:
                    stats['ok'] += 1
                    display.log(f'[{i}/{total}] OK     {video.name}  ({dt:.1f}s)')
                else:
                    stats['fail'] += 1
                    display.log(f'[{i}/{total}] FAIL   {video.name}')
                    if err:
                        last = err.strip().splitlines()[-1] if err.strip() else ''
                        if last:
                            display.log(f'         {last[:120]}')
            update_overall()

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(num_workers)]
    for t in threads:
        t.start()
    update_overall()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print('\n\nInterrupted. Waiting briefly for workers...', file=sys.stderr)
        # Daemon threads will die when the process exits; ffmpeg children
        # receive SIGTERM on Unix, may linger on Windows — acceptable.
        raise

    return stats['ok'], stats['fail']


# -------- CLI / GUI --------

def pick_directory_gui():
    try:
        import tkinter as tk
        from tkinter import filedialog
    except ImportError:
        print('tkinter not available; cannot open folder picker.', file=sys.stderr)
        return None
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    folder = filedialog.askdirectory(title='Select folder with video and audio files')
    root.destroy()
    return Path(folder) if folder else None


def run(args):
    if not shutil.which('ffmpeg'):
        print('Error: ffmpeg not found on PATH.\n'
              '  Windows:  winget install ffmpeg   (in PowerShell)\n'
              '  Ubuntu:   sudo apt install ffmpeg', file=sys.stderr)
        return

    if not args.directory.is_dir():
        print(f'Error: not a directory: {args.directory}', file=sys.stderr)
        return

    videos, audios = collect_files(args.directory)
    print(f'Folder: {args.directory}')
    print(f'Found {len(videos)} video file(s) and {len(audios)} audio file(s).\n')

    if args.dry_run:
        print('Videos (detected episode key in brackets):')
        for v in videos:
            print(f'  [{extract_episode(v.stem) or "???"}]  {v.name}')
        print('\nAudios:')
        for a in audios:
            print(f'  [{extract_episode(a.stem) or "???"}]  {a.name}')
        print()

    if not videos or not audios:
        print('Nothing to merge.')
        return

    pairs, method, ambiguous = find_pairs(videos, audios)
    if ambiguous:
        print(f'Warning: skipped ambiguous matches for episodes: {", ".join(ambiguous)}\n',
              file=sys.stderr)

    if not pairs:
        print('No pairs found.\n'
              'Files must share a base name OR a detectable episode number.\n'
              'Run with --dry-run to see what episode keys were detected.')
        return

    print(f'Matched {len(pairs)} pair(s) by {method}:')
    for v, a in pairs:
        print(f'  {v.name}\n    + {a.name}')

    if args.dry_run:
        print('\n[dry-run] nothing written.')
        return

    output_dir = args.output or (args.directory / 'merged')
    output_dir.mkdir(parents=True, exist_ok=True)

    jobs = max(1, args.jobs)
    # Don't spawn more workers than files
    jobs = min(jobs, len(pairs))

    print()
    if jobs == 1:
        ok, fail = process_serial(pairs, args, output_dir)
    else:
        ok, fail = process_parallel(pairs, args, output_dir, jobs)

    print(f'\nDone. {ok} succeeded, {fail} failed. Output: {output_dir}')


def main():
    parser = argparse.ArgumentParser(
        description='Batch merge external audio tracks into video files.')
    parser.add_argument('directory', type=Path, nargs='?', default=None,
                        help='Directory with video/audio pairs. '
                             'If omitted, a folder picker dialog opens.')
    parser.add_argument('-o', '--output', type=Path,
                        help='Output directory (default: <directory>/merged)')
    parser.add_argument('-j', '--jobs', type=int, default=1,
                        help='Number of parallel merges (default: 1). '
                             'Try 2-4 on SSD; on HDD stick to 1.')
    parser.add_argument('--keep-original', action='store_true',
                        help='Keep original video audio track(s) as well')
    parser.add_argument('--audio-codec', default='copy',
                        help='Audio codec: "copy" (default) or e.g. "aac" for .mp4 output')
    parser.add_argument('--ext', default='mkv',
                        help='Output container extension (default: mkv)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show detected files and pairings without running ffmpeg')
    args = parser.parse_args()

    gui_mode = args.directory is None
    if gui_mode:
        print('No folder specified. Opening folder picker...')
        picked = pick_directory_gui()
        if not picked:
            print('Cancelled.')
        else:
            args.directory = picked
            try:
                run(args)
            except KeyboardInterrupt:
                print('\nAborted by user.', file=sys.stderr)
            except Exception as e:
                print(f'\nUnexpected error: {e}', file=sys.stderr)
        input('\nPress Enter to close...')
    else:
        try:
            run(args)
        except KeyboardInterrupt:
            sys.exit('\nAborted by user.')


if __name__ == '__main__':
    main()