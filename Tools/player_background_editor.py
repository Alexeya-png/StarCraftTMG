from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
except ImportError as exc:  # pragma: no cover - startup guard for local desktops
    print(f'Tkinter is not available: {exc}', file=sys.stderr)
    sys.exit(1)

try:
    from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageOps, ImageTk
except ImportError:  # pragma: no cover - handled in main()
    Image = None
    ImageChops = None
    ImageDraw = None
    ImageFilter = None
    ImageFont = None
    ImageOps = None
    ImageTk = None


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIR = Path.home() / 'Desktop' / 'photo'
DEFAULT_OUTPUT_DIR = ROOT / 'app' / 'static' / 'art' / 'profile-banners'
BACKGROUND_JSON = ROOT / 'app' / 'static' / 'art' / 'player-backgrounds.json'

EXPORT_WIDTH = 2400
EXPORT_HEIGHT = 780
PREVIEW_WIDTH = 720
PREVIEW_HEIGHT = round(PREVIEW_WIDTH * EXPORT_HEIGHT / EXPORT_WIDTH)
MIN_CROP_OVERLAP = 48.0
OUT_OF_BOUNDS_FADE_RATIO = 0.18
WEBP_QUALITY = 92
MIN_ZOOM = 0.25
MAX_ZOOM = 5.0

RACE_PREFIX = {
    'protoss': 'p',
    'terran': 't',
    'zerg': 'z',
}

RACE_ACCENTS = {
    'protoss': ((56, 246, 208), (246, 213, 107), '#09071a'),
    'terran': ((67, 183, 255), (255, 143, 61), '#071322'),
    'zerg': ((185, 93, 255), (255, 96, 72), '#170819'),
}


def parse_int(value: str, fallback: int) -> int:
    try:
        parsed = int(str(value).strip())
    except ValueError:
        return fallback
    return max(64, parsed)


def load_font(size: int, bold: bool = False) -> Any:
    if ImageFont is None:
        return None

    candidates = [
        'C:/Windows/Fonts/arialbd.ttf' if bold else 'C:/Windows/Fonts/arial.ttf',
        'C:/Windows/Fonts/segoeuib.ttf' if bold else 'C:/Windows/Fonts/segoeui.ttf',
    ]
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            pass
    return ImageFont.load_default()


def alpha_gradient(width: int, height: int, axis: str, stops: list[tuple[float, int]]) -> Any:
    if Image is None:
        return None

    stops = sorted(stops, key=lambda item: item[0])
    length = width if axis == 'x' else height
    pixels = []
    for index in range(length):
        point = index / max(1, length - 1)
        left = stops[0]
        right = stops[-1]
        for stop_index in range(len(stops) - 1):
            if stops[stop_index][0] <= point <= stops[stop_index + 1][0]:
                left = stops[stop_index]
                right = stops[stop_index + 1]
                break
        span = max(0.0001, right[0] - left[0])
        mix = min(1.0, max(0.0, (point - left[0]) / span))
        alpha = round(left[1] + (right[1] - left[1]) * mix)
        pixels.append((3, 6, 14, alpha))

    if axis == 'x':
        strip = Image.new('RGBA', (width, 1))
        strip.putdata(pixels)
        return strip.resize((width, height))

    strip = Image.new('RGBA', (1, height))
    strip.putdata(pixels)
    return strip.resize((width, height))


def apply_banner_grade(image: Any, race: str) -> Any:
    primary, secondary, shadow = RACE_ACCENTS.get(race, RACE_ACCENTS['protoss'])
    base = image.convert('RGBA')
    width, height = base.size

    shadow_layer = Image.new('RGBA', base.size, shadow)
    base = Image.blend(shadow_layer, base, 0.92)

    glow = Image.new('RGBA', base.size, (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow)
    glow_draw.ellipse(
        (round(width * 0.36), round(height * -0.35), round(width * 1.18), round(height * 1.22)),
        fill=(*primary, 42),
    )
    glow_draw.ellipse(
        (round(width * 0.62), round(height * -0.15), round(width * 1.12), round(height * 0.92)),
        fill=(*secondary, 22),
    )
    glow = glow.filter(ImageFilter.GaussianBlur(radius=max(20, height // 7)))
    base = Image.alpha_composite(base, glow)

    base = Image.alpha_composite(
        base,
        alpha_gradient(
            width,
            height,
            'x',
            [(0.0, 188), (0.24, 56), (0.56, 8), (0.82, 66), (1.0, 205)],
        ),
    )
    base = Image.alpha_composite(
        base,
        alpha_gradient(
            width,
            height,
            'y',
            [(0.0, 0), (0.32, 0), (1.0, 128)],
        ),
    )
    return base.convert('RGB')


def edge_fade_mask(size: tuple[int, int], edges: set[str], fade_size: int) -> Any:
    if Image is None or ImageChops is None:
        return None

    width, height = size
    mask = Image.new('L', size, 255)

    def multiply(edge_mask: Any) -> None:
        nonlocal mask
        mask = ImageChops.multiply(mask, edge_mask)

    if 'left' in edges:
        fade = max(1, min(fade_size, width))
        strip = Image.new('L', (fade, 1))
        strip.putdata([round(255 * index / max(1, fade - 1)) for index in range(fade)])
        edge = Image.new('L', size, 255)
        edge.paste(strip.resize((fade, height)), (0, 0))
        multiply(edge)

    if 'right' in edges:
        fade = max(1, min(fade_size, width))
        strip = Image.new('L', (fade, 1))
        strip.putdata([round(255 * (1 - index / max(1, fade - 1))) for index in range(fade)])
        edge = Image.new('L', size, 255)
        edge.paste(strip.resize((fade, height)), (width - fade, 0))
        multiply(edge)

    if 'top' in edges:
        fade = max(1, min(fade_size, height))
        strip = Image.new('L', (1, fade))
        strip.putdata([round(255 * index / max(1, fade - 1)) for index in range(fade)])
        edge = Image.new('L', size, 255)
        edge.paste(strip.resize((width, fade)), (0, 0))
        multiply(edge)

    if 'bottom' in edges:
        fade = max(1, min(fade_size, height))
        strip = Image.new('L', (1, fade))
        strip.putdata([round(255 * (1 - index / max(1, fade - 1))) for index in range(fade)])
        edge = Image.new('L', size, 255)
        edge.paste(strip.resize((width, fade)), (0, height - fade))
        multiply(edge)

    return mask


def crop_on_dark_background(source: Any, crop_rect: tuple[float, float, float, float], race: str) -> Any:
    _primary, _secondary, shadow = RACE_ACCENTS.get(race, RACE_ACCENTS['protoss'])
    left, top, right, bottom = crop_rect
    crop_left = round(left)
    crop_top = round(top)
    crop_w = max(1, round(right - left))
    crop_h = max(1, round(bottom - top))
    crop_right = crop_left + crop_w
    crop_bottom = crop_top + crop_h

    canvas = Image.new('RGBA', (crop_w, crop_h), shadow)
    src_left = max(0, crop_left)
    src_top = max(0, crop_top)
    src_right = min(source.width, crop_right)
    src_bottom = min(source.height, crop_bottom)
    if src_right <= src_left or src_bottom <= src_top:
        return canvas.convert('RGB')

    piece = source.crop((src_left, src_top, src_right, src_bottom)).convert('RGBA')
    fade_edges = set()
    if crop_left < 0:
        fade_edges.add('left')
    if crop_right > source.width:
        fade_edges.add('right')
    if crop_top < 0:
        fade_edges.add('top')
    if crop_bottom > source.height:
        fade_edges.add('bottom')

    if fade_edges:
        fade_size = max(12, round(min(crop_w, crop_h) * OUT_OF_BOUNDS_FADE_RATIO))
        mask = edge_fade_mask(piece.size, fade_edges, fade_size)
        if mask is not None:
            piece.putalpha(mask)

    canvas.alpha_composite(piece, (src_left - crop_left, src_top - crop_top))
    return canvas.convert('RGB')


class PlayerBackgroundEditor:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title('TMG Player Background Editor')
        self.root.minsize(1180, 760)

        self.source_image: Any | None = None
        self.source_path: Path | None = None
        self.crop_rect: tuple[float, float, float, float] | None = None
        self.base_crop_size: tuple[float, float] = (1.0, 1.0)
        self.drag_last: tuple[float, float] | None = None

        self.canvas_photo: Any | None = None
        self.preview_photo: Any | None = None
        self.image_scale = 1.0
        self.image_offset = (0.0, 0.0)

        self.race_var = tk.StringVar(value='protoss')
        self.output_var = tk.StringVar(value='p1.webp')
        self.output_dir_var = tk.StringVar(value=str(DEFAULT_OUTPUT_DIR))
        self.width_var = tk.StringVar(value=str(EXPORT_WIDTH))
        self.height_var = tk.StringVar(value=str(EXPORT_HEIGHT))
        self.zoom_var = tk.DoubleVar(value=1.0)
        self.grade_var = tk.BooleanVar(value=True)
        self.register_json_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value='Open a source image to start.')

        self._build_ui()
        self._bind_events()
        self.redraw_all()

    def _build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=12)
        outer.grid(row=0, column=0, sticky='nsew')
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        toolbar = ttk.Frame(outer)
        toolbar.grid(row=0, column=0, columnspan=2, sticky='ew', pady=(0, 10))
        toolbar.columnconfigure(8, weight=1)

        ttk.Button(toolbar, text='Open photo', command=self.open_image).grid(row=0, column=0, padx=(0, 8))
        ttk.Label(toolbar, text='Race').grid(row=0, column=1, padx=(0, 5))
        race_box = ttk.Combobox(
            toolbar,
            textvariable=self.race_var,
            values=('protoss', 'terran', 'zerg'),
            width=10,
            state='readonly',
        )
        race_box.grid(row=0, column=2, padx=(0, 8))
        ttk.Button(toolbar, text='Next slot', command=self.suggest_next_slot).grid(row=0, column=3, padx=(0, 8))
        ttk.Label(toolbar, text='Output').grid(row=0, column=4, padx=(0, 5))
        ttk.Entry(toolbar, textvariable=self.output_var, width=18).grid(row=0, column=5, padx=(0, 8))
        ttk.Button(toolbar, text='Export WEBP', command=self.export_image).grid(row=0, column=6, padx=(0, 8))
        ttk.Button(toolbar, text='Output folder', command=self.choose_output_dir).grid(row=0, column=7)

        main = ttk.Frame(outer)
        main.grid(row=1, column=0, columnspan=2, sticky='nsew')
        main.columnconfigure(0, weight=3)
        main.columnconfigure(1, weight=2)
        main.rowconfigure(0, weight=1)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(1, weight=1)

        crop_frame = ttk.LabelFrame(main, text='Crop source')
        crop_frame.grid(row=0, column=0, sticky='nsew', padx=(0, 10))
        crop_frame.columnconfigure(0, weight=1)
        crop_frame.rowconfigure(0, weight=1)
        self.canvas = tk.Canvas(crop_frame, bg='#080c16', highlightthickness=0, cursor='crosshair')
        self.canvas.grid(row=0, column=0, sticky='nsew')

        side = ttk.Frame(main)
        side.grid(row=0, column=1, sticky='nsew')
        side.columnconfigure(0, weight=1)

        preview_frame = ttk.LabelFrame(side, text='Profile preview')
        preview_frame.grid(row=0, column=0, sticky='ew')
        preview_frame.columnconfigure(0, weight=1)
        self.preview_canvas = tk.Canvas(
            preview_frame,
            width=PREVIEW_WIDTH,
            height=PREVIEW_HEIGHT,
            bg='#080c16',
            highlightthickness=0,
        )
        self.preview_canvas.grid(row=0, column=0, padx=10, pady=10)

        controls = ttk.LabelFrame(side, text='Export settings')
        controls.grid(row=1, column=0, sticky='ew', pady=(10, 0))
        for column in range(4):
            controls.columnconfigure(column, weight=1)

        ttk.Label(controls, text='Width').grid(row=0, column=0, sticky='w', padx=10, pady=(10, 3))
        ttk.Entry(controls, textvariable=self.width_var, width=8).grid(row=1, column=0, sticky='ew', padx=10)
        ttk.Label(controls, text='Height').grid(row=0, column=1, sticky='w', padx=10, pady=(10, 3))
        ttk.Entry(controls, textvariable=self.height_var, width=8).grid(row=1, column=1, sticky='ew', padx=10)
        ttk.Checkbutton(controls, text='Dark UI grade', variable=self.grade_var, command=self.redraw_preview).grid(
            row=1,
            column=2,
            columnspan=2,
            sticky='w',
            padx=10,
        )
        ttk.Checkbutton(
            controls,
            text='Update JSON mapping',
            variable=self.register_json_var,
        ).grid(row=2, column=0, columnspan=4, sticky='w', padx=10, pady=(8, 0))

        ttk.Label(controls, text='Zoom / crop size').grid(row=3, column=0, columnspan=4, sticky='w', padx=10, pady=(12, 3))
        ttk.Scale(
            controls,
            from_=MIN_ZOOM,
            to=MAX_ZOOM,
            variable=self.zoom_var,
            command=lambda _value: self.on_zoom_changed(),
        ).grid(row=4, column=0, columnspan=4, sticky='ew', padx=10, pady=(0, 10))

        help_text = (
            'Mouse: drag crop box beyond photo edges, click to recenter, wheel to zoom.\n'
            'Keys: arrows move crop, +/- zoom.\n'
            'Export size is profile banner aspect; default is 2400x780.'
        )
        ttk.Label(side, text=help_text, justify='left').grid(row=2, column=0, sticky='ew', pady=(12, 0))

        status = ttk.Label(outer, textvariable=self.status_var, anchor='w')
        status.grid(row=2, column=0, columnspan=2, sticky='ew', pady=(10, 0))

    def _bind_events(self) -> None:
        self.canvas.bind('<Configure>', lambda _event: self.redraw_canvas())
        self.canvas.bind('<ButtonPress-1>', self.on_canvas_press)
        self.canvas.bind('<B1-Motion>', self.on_canvas_drag)
        self.canvas.bind('<ButtonRelease-1>', self.on_canvas_release)
        self.canvas.bind('<MouseWheel>', self.on_mouse_wheel)
        self.root.bind('<Left>', lambda _event: self.nudge_crop(-18, 0))
        self.root.bind('<Right>', lambda _event: self.nudge_crop(18, 0))
        self.root.bind('<Up>', lambda _event: self.nudge_crop(0, -18))
        self.root.bind('<Down>', lambda _event: self.nudge_crop(0, 18))
        self.root.bind('<plus>', lambda _event: self.zoom_by(1.08))
        self.root.bind('<minus>', lambda _event: self.zoom_by(1 / 1.08))
        self.race_var.trace_add('write', lambda *_args: self.redraw_preview())
        self.width_var.trace_add('write', lambda *_args: self.reset_crop_for_aspect())
        self.height_var.trace_add('write', lambda *_args: self.reset_crop_for_aspect())

    def open_image(self) -> None:
        initial_dir = DEFAULT_SOURCE_DIR if DEFAULT_SOURCE_DIR.exists() else Path.home()
        file_path = filedialog.askopenfilename(
            title='Choose source photo',
            initialdir=str(initial_dir),
            filetypes=(
                ('Images', '*.jpg *.jpeg *.png *.webp *.bmp'),
                ('All files', '*.*'),
            ),
        )
        if not file_path:
            return

        try:
            image = ImageOps.exif_transpose(Image.open(file_path)).convert('RGB')
        except Exception as exc:
            messagebox.showerror('Cannot open image', str(exc))
            return

        self.source_image = image
        self.source_path = Path(file_path)
        self.zoom_var.set(1.0)
        self.reset_crop_for_aspect()
        self.status_var.set(f'Loaded {self.source_path.name} ({image.width}x{image.height}).')
        self.redraw_all()

    def choose_output_dir(self) -> None:
        directory = filedialog.askdirectory(
            title='Choose output folder',
            initialdir=self.output_dir_var.get() or str(DEFAULT_OUTPUT_DIR),
        )
        if directory:
            self.output_dir_var.set(directory)

    def suggest_next_slot(self) -> None:
        race = self.race_var.get()
        prefix = RACE_PREFIX.get(race, 'bg')
        output_dir = Path(self.output_dir_var.get() or DEFAULT_OUTPUT_DIR)
        used = set()
        if output_dir.exists():
            for extension in ('webp', 'jpg', 'jpeg'):
                for file_path in output_dir.glob(f'{prefix}*.{extension}'):
                    suffix = file_path.stem[len(prefix):]
                    if suffix.isdigit():
                        used.add(int(suffix))
        index = 1
        while index in used:
            index += 1
        self.output_var.set(f'{prefix}{index}.webp')

    def reset_crop_for_aspect(self) -> None:
        if self.source_image is None:
            return
        width = parse_int(self.width_var.get(), EXPORT_WIDTH)
        height = parse_int(self.height_var.get(), EXPORT_HEIGHT)
        target_aspect = width / height
        image_aspect = self.source_image.width / self.source_image.height
        if image_aspect > target_aspect:
            base_h = float(self.source_image.height)
            base_w = base_h * target_aspect
        else:
            base_w = float(self.source_image.width)
            base_h = base_w / target_aspect
        self.base_crop_size = (base_w, base_h)
        self.update_crop(center=(self.source_image.width / 2, self.source_image.height / 2))

    def update_crop(self, center: tuple[float, float] | None = None) -> None:
        if self.source_image is None:
            return
        if center is None and self.crop_rect is not None:
            left, top, right, bottom = self.crop_rect
            center = ((left + right) / 2, (top + bottom) / 2)
        elif center is None:
            center = (self.source_image.width / 2, self.source_image.height / 2)

        zoom = min(MAX_ZOOM, max(MIN_ZOOM, float(self.zoom_var.get() or 1.0)))
        crop_w = max(16.0, self.base_crop_size[0] / zoom)
        crop_h = max(16.0, self.base_crop_size[1] / zoom)

        left = center[0] - crop_w / 2
        top = center[1] - crop_h / 2
        min_overlap_x = min(MIN_CROP_OVERLAP, crop_w, float(self.source_image.width))
        min_overlap_y = min(MIN_CROP_OVERLAP, crop_h, float(self.source_image.height))
        left = min(max(-crop_w + min_overlap_x, left), self.source_image.width - min_overlap_x)
        top = min(max(-crop_h + min_overlap_y, top), self.source_image.height - min_overlap_y)
        self.crop_rect = (left, top, left + crop_w, top + crop_h)
        self.redraw_all()

    def canvas_to_image(self, x_pos: float, y_pos: float) -> tuple[float, float] | None:
        if self.source_image is None:
            return None
        offset_x, offset_y = self.image_offset
        if self.image_scale <= 0:
            return None
        x_value = (x_pos - offset_x) / self.image_scale
        y_value = (y_pos - offset_y) / self.image_scale
        return (x_value, y_value)

    def on_canvas_press(self, event: tk.Event) -> None:
        point = self.canvas_to_image(event.x, event.y)
        if point is None:
            return
        if self.crop_rect is not None:
            left, top, right, bottom = self.crop_rect
            if not (left <= point[0] <= right and top <= point[1] <= bottom):
                self.update_crop(center=point)
        self.drag_last = point

    def on_canvas_drag(self, event: tk.Event) -> None:
        if self.drag_last is None or self.crop_rect is None:
            return
        point = self.canvas_to_image(event.x, event.y)
        if point is None:
            return
        delta_x = point[0] - self.drag_last[0]
        delta_y = point[1] - self.drag_last[1]
        left, top, right, bottom = self.crop_rect
        center = ((left + right) / 2 + delta_x, (top + bottom) / 2 + delta_y)
        self.drag_last = point
        self.update_crop(center=center)

    def on_canvas_release(self, _event: tk.Event) -> None:
        self.drag_last = None

    def on_mouse_wheel(self, event: tk.Event) -> None:
        self.zoom_by(1.08 if event.delta > 0 else 1 / 1.08)

    def on_zoom_changed(self) -> None:
        self.update_crop()

    def zoom_by(self, factor: float) -> None:
        value = min(MAX_ZOOM, max(MIN_ZOOM, float(self.zoom_var.get() or 1.0) * factor))
        self.zoom_var.set(value)
        self.update_crop()

    def nudge_crop(self, dx: float, dy: float) -> None:
        if self.crop_rect is None:
            return
        left, top, right, bottom = self.crop_rect
        center = ((left + right) / 2 + dx, (top + bottom) / 2 + dy)
        self.update_crop(center=center)

    def redraw_all(self) -> None:
        self.redraw_canvas()
        self.redraw_preview()
        self.update_status_crop()

    def redraw_canvas(self) -> None:
        self.canvas.delete('all')
        width = max(1, self.canvas.winfo_width())
        height = max(1, self.canvas.winfo_height())
        if self.source_image is None:
            self.canvas.create_text(
                width / 2,
                height / 2,
                text='Open a photo to crop a player background',
                fill='#b4bedf',
                font=('Arial', 16, 'bold'),
            )
            return

        scale = min(width / self.source_image.width, height / self.source_image.height)
        display_w = max(1, round(self.source_image.width * scale))
        display_h = max(1, round(self.source_image.height * scale))
        offset_x = (width - display_w) / 2
        offset_y = (height - display_h) / 2
        self.image_scale = scale
        self.image_offset = (offset_x, offset_y)

        display_image = self.source_image.resize((display_w, display_h), Image.Resampling.LANCZOS)
        self.canvas_photo = ImageTk.PhotoImage(display_image)
        self.canvas.create_image(offset_x, offset_y, anchor='nw', image=self.canvas_photo)

        if self.crop_rect is None:
            return

        left, top, right, bottom = self.crop_rect
        x1 = offset_x + left * scale
        y1 = offset_y + top * scale
        x2 = offset_x + right * scale
        y2 = offset_y + bottom * scale

        self.canvas.create_rectangle(0, 0, width, y1, fill='#02050b', stipple='gray50', outline='')
        self.canvas.create_rectangle(0, y2, width, height, fill='#02050b', stipple='gray50', outline='')
        self.canvas.create_rectangle(0, y1, x1, y2, fill='#02050b', stipple='gray50', outline='')
        self.canvas.create_rectangle(x2, y1, width, y2, fill='#02050b', stipple='gray50', outline='')
        self.canvas.create_rectangle(x1, y1, x2, y2, outline='#8baeff', width=3)
        self.canvas.create_rectangle(x1 + 4, y1 + 4, x2 - 4, y2 - 4, outline='#ffffff', width=1)

    def make_banner_image(self, size: tuple[int, int]) -> Any:
        if self.source_image is None or self.crop_rect is None:
            raise RuntimeError('No crop is selected.')
        crop = crop_on_dark_background(self.source_image, self.crop_rect, self.race_var.get())
        banner = crop.resize(size, Image.Resampling.LANCZOS)
        if self.grade_var.get():
            banner = apply_banner_grade(banner, self.race_var.get())
        return banner.convert('RGB')

    def redraw_preview(self) -> None:
        self.preview_canvas.delete('all')
        if self.source_image is None or self.crop_rect is None:
            self.preview_canvas.create_text(
                PREVIEW_WIDTH / 2,
                PREVIEW_HEIGHT / 2,
                text='Preview will appear here',
                fill='#b4bedf',
                font=('Arial', 14, 'bold'),
            )
            return

        banner = self.make_banner_image((PREVIEW_WIDTH, PREVIEW_HEIGHT))
        preview = self.render_profile_mockup(banner)
        self.preview_photo = ImageTk.PhotoImage(preview)
        self.preview_canvas.create_image(0, 0, anchor='nw', image=self.preview_photo)

    def render_profile_mockup(self, banner: Any) -> Any:
        race = self.race_var.get()
        primary, _secondary, _shadow = RACE_ACCENTS.get(race, RACE_ACCENTS['protoss'])
        image = banner.convert('RGBA')
        width, height = image.size

        overlay = Image.new('RGBA', image.size, (0, 0, 0, 0))
        overlay = Image.alpha_composite(
            overlay,
            alpha_gradient(width, height, 'x', [(0.0, 218), (0.42, 72), (0.72, 86), (1.0, 220)]),
        )
        overlay = Image.alpha_composite(
            overlay,
            alpha_gradient(width, height, 'y', [(0.0, 20), (0.62, 26), (1.0, 170)]),
        )
        image = Image.alpha_composite(image, overlay)

        draw = ImageDraw.Draw(image)
        border = (*primary, 80)
        draw.rounded_rectangle((8, 8, width - 8, height - 8), radius=18, outline=border, width=1)

        small_font = load_font(12, bold=True)
        title_font = load_font(34, bold=True)
        meta_font = load_font(15, bold=False)
        value_font = load_font(26, bold=True)

        draw.rounded_rectangle((24, 28, 148, 64), radius=11, fill=(4, 8, 18, 150), outline=(139, 174, 255, 78), width=1)
        draw.text((42, 38), '<- Back', fill=(245, 248, 255, 235), font=small_font)

        flag_x = 40
        name_y = 95
        draw.rounded_rectangle((flag_x, name_y + 8, flag_x + 28, name_y + 20), radius=2, fill=(255, 255, 255, 255))
        draw.rectangle((flag_x, name_y + 14, flag_x + 28, name_y + 20), fill=(220, 26, 65, 255))
        draw.text((flag_x + 42, name_y - 2), 'Preview Player', fill=(246, 249, 255, 255), font=title_font)
        draw.text((flag_x, name_y + 46), f'Rank #1 - {race.title()}', fill=(195, 206, 234, 230), font=meta_font)

        badge_path = ROOT / 'app' / 'static' / 'badges' / f'contender-{race}.webp'
        if badge_path.exists():
            try:
                badge = Image.open(badge_path).convert('RGBA')
                badge.thumbnail((84, 84), Image.Resampling.LANCZOS)
                image.alpha_composite(badge, (round(width * 0.38), 68))
            except Exception:
                pass

        card_x = width - 134
        card_y = 74
        draw.rounded_rectangle((card_x, card_y, width - 28, card_y + 80), radius=14, fill=(4, 8, 18, 156), outline=(221, 232, 255, 42), width=1)
        draw.text((card_x + 30, card_y + 16), 'ELO', fill=(195, 206, 234, 225), font=meta_font)
        draw.text((card_x + 28, card_y + 38), '1150', fill=(246, 249, 255, 255), font=value_font)
        return image.convert('RGB')

    def update_status_crop(self) -> None:
        if self.source_image is None or self.crop_rect is None:
            return
        left, top, right, bottom = self.crop_rect
        self.status_var.set(
            f'Crop: {round(left)}, {round(top)} -> {round(right)}, {round(bottom)} | '
            f'Zoom: {self.zoom_var.get():.2f} | Output: {self.output_var.get()}'
        )

    def export_image(self) -> None:
        if self.source_image is None or self.crop_rect is None:
            messagebox.showinfo('No source image', 'Open a photo first.')
            return

        output_name = self.output_var.get().strip() or 'player-background.webp'
        if not output_name.lower().endswith('.webp'):
            output_name = f'{Path(output_name).stem}.webp'

        output_dir = Path(self.output_dir_var.get() or DEFAULT_OUTPUT_DIR)
        output_path = output_dir / output_name
        export_size = (
            parse_int(self.width_var.get(), EXPORT_WIDTH),
            parse_int(self.height_var.get(), EXPORT_HEIGHT),
        )

        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            banner = self.make_banner_image(export_size)
            banner.save(output_path, format='WEBP', quality=WEBP_QUALITY, method=6)
            self.update_json_mapping(output_path)
        except Exception as exc:
            messagebox.showerror('Export failed', str(exc))
            return

        self.status_var.set(f'Exported {output_path}')
        messagebox.showinfo('Export complete', f'Saved:\n{output_path}')

    def update_json_mapping(self, output_path: Path) -> None:
        if not self.register_json_var.get():
            return
        try:
            if output_path.parent.resolve() != DEFAULT_OUTPUT_DIR.resolve():
                return
        except OSError:
            return

        background_id = output_path.stem
        race = self.race_var.get()
        try:
            config = json.loads(BACKGROUND_JSON.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            config = {'version': 2, 'backgrounds': {}, 'race_variants': {}, 'race_defaults': {}, 'players': {}}

        if not isinstance(config, dict):
            config = {'version': 2, 'backgrounds': {}, 'race_variants': {}, 'race_defaults': {}, 'players': {}}

        config['version'] = max(2, int(config.get('version') or 2))
        backgrounds = config.setdefault('backgrounds', {})
        variants = config.setdefault('race_variants', {})
        defaults = config.setdefault('race_defaults', {})

        backgrounds[background_id] = {
            'name': f'{race.title()} {background_id}',
            'race': race,
            'url': f'/static/art/profile-banners/{output_path.name}',
            'position': 'center 50%',
        }

        race_variants = variants.setdefault(race, [])
        if isinstance(race_variants, list) and background_id not in race_variants:
            race_variants.append(background_id)
        defaults.setdefault(race, background_id)

        BACKGROUND_JSON.write_text(json.dumps(config, indent=2) + '\n', encoding='utf-8')


def main() -> int:
    if Image is None:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(
            'Pillow is required',
            'Install Pillow first:\n\npy -m pip install Pillow\n\nor reinstall project requirements.',
        )
        return 1

    root = tk.Tk()
    app = PlayerBackgroundEditor(root)
    root.mainloop()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())