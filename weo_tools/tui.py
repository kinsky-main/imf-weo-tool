from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from itertools import cycle
from queue import Empty, Queue
import sys
from threading import Event, Thread
from textwrap import wrap
from time import sleep
from typing import Any, Callable, TypeVar

from prompt_toolkit.application import Application
from prompt_toolkit.application.current import get_app_or_none
from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.key_binding.key_bindings import merge_key_bindings
from prompt_toolkit.layout import Dimension, HSplit, Layout, Window
from prompt_toolkit.layout.containers import ConditionalContainer, VSplit
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.margins import ScrollbarMargin
from prompt_toolkit.mouse_events import MouseEvent, MouseEventType
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import Frame, TextArea

from .legacy import normalize_label


T = TypeVar("T")
_ACTIVE_SESSION: "_InteractiveTuiSession | None" = None


@dataclass(frozen=True, slots=True)
class Choice:
    name: str
    value: str
    checked: bool = False
    meta: str = ""
    detail: str = ""


@dataclass(slots=True)
class _PromptRequest:
    title: str
    choices: list[Choice]
    required: bool
    max_selections: int | None
    done: Event
    result: list[str] | None = None
    error: BaseException | None = None


@dataclass(slots=True)
class _RangePromptRequest:
    title: str
    start_value: str
    end_value: str
    start_placeholder: str
    end_placeholder: str
    caption: str
    validate: Callable[[str, str], tuple[str, str]]
    done: Event
    result: tuple[str, str] | None = None
    error: BaseException | None = None


class SearchableMultiSelect:
    def __init__(
        self,
        title: str,
        choices: list[Choice],
        required: bool = True,
        max_selections: int | None = None,
        *,
        before_render: Callable[[], None] | None = None,
        on_submit: Callable[[list[str]], None] | None = None,
        on_cancel: Callable[[], None] | None = None,
        is_active: Callable[[], bool] | None = None,
    ) -> None:
        self.title = title
        self.choices = choices
        self.required = required
        self.max_selections = max_selections
        self.selected = {choice.value for choice in choices if choice.checked}
        self.cursor = 0
        self.error_message = ""
        self.search_focus_on_click = True
        self.summary_lines: list[str] = []
        self.status_message = ""
        self.loading = False
        self._before_render = before_render
        self._on_submit = on_submit
        self._on_cancel = on_cancel
        self._is_active = is_active or (lambda: True)
        self._spinner_symbols = cycle("|/-\\")
        self._render_hook_active = False

        self.search = TextArea(
            prompt="Search: ",
            multiline=False,
            wrap_lines=False,
            focus_on_click=self.search_focus_on_click,
        )
        self.search.buffer.on_text_changed += self._reset_cursor
        self.list_control = FormattedTextControl(self._render_choices, focusable=True)
        self.list_window = Window(
            content=self.list_control,
            wrap_lines=False,
            always_hide_cursor=True,
            height=Dimension(min=10),
            right_margins=[ScrollbarMargin(display_arrows=True)],
        )
        self.error_control = FormattedTextControl(self._render_error)
        self.summary_control = FormattedTextControl(self._render_summary)
        self.status_control = FormattedTextControl(self._render_status)
        self.title_window = self._build_title_window()
        self.summary_window = self._build_summary_window()
        self.search_frame = self._build_search_frame()
        self.matches_frame = self._build_matches_frame()
        self.status_window = self._build_status_window()
        self.error_window = self._build_error_window()
        self.bindings = self._bindings()
        self.style = Style.from_dict(
            {
                "title": "fg:ansiwhite bold",
                "frame.label": "fg:ansicyan bold",
                "active": "bg:ansiblack fg:ansiyellow bold",
                "checked": "fg:ansigreen",
                "checked-active": "bg:ansiblack fg:ansigreen bold",
                "meta": "fg:ansiblue",
                "meta-active": "bg:ansiblack fg:ansiblue bold",
                "checked-meta": "fg:ansicyan",
                "checked-meta-active": "bg:ansiblack fg:ansicyan bold",
                "detail": "fg:ansibrightblack",
                "detail-active": "bg:ansiblack fg:ansicyan",
                "checked-detail": "fg:ansibrightblack",
                "checked-detail-active": "bg:ansiblack fg:ansicyan bold",
                "muted": "fg:ansibrightblack italic",
                "status": "fg:ansicyan",
                "error": "fg:ansired bold",
            }
        )

    def configure_prompt(
        self,
        *,
        title: str,
        choices: list[Choice],
        required: bool,
        max_selections: int | None,
    ) -> None:
        self.title = title
        self.choices = choices
        self.required = required
        self.max_selections = max_selections
        self.selected = {choice.value for choice in choices if choice.checked}
        self.error_message = ""
        self.search.buffer.text = ""
        self.cursor = 0 if choices else -1

    def set_summary_lines(self, lines: list[str]) -> None:
        self.summary_lines = list(lines)

    def set_status(self, message: str, *, loading: bool) -> None:
        self.status_message = message
        self.loading = loading
        if not loading:
            self.error_message = ""

    def run(self) -> list[str]:
        application = self.build_application()
        result = application.run()
        if result is None:
            raise KeyboardInterrupt("Selection cancelled.")
        return result

    def build_application(self) -> Application:
        root = self._build_root_container()
        return Application(
            layout=Layout(root, focused_element=self.search),
            key_bindings=self.bindings,
            full_screen=True,
            mouse_support=True,
            style=self.style,
            refresh_interval=0.1,
        )

    def _maybe_before_render(self) -> None:
        if self._before_render is None or self._render_hook_active:
            return
        self._render_hook_active = True
        try:
            self._before_render()
        finally:
            self._render_hook_active = False

    def _build_title_window(self) -> Window:
        return Window(
            content=FormattedTextControl(self._render_title),
            height=2,
        )

    def _build_summary_window(self) -> ConditionalContainer:
        return ConditionalContainer(
            Window(content=self.summary_control, height=4),
            filter=Condition(lambda: bool(self.summary_lines)),
        )

    def _build_search_frame(self) -> Frame:
        return Frame(self.search, title="Search")

    def _build_matches_frame(self) -> Frame:
        return Frame(self.list_window, title="Matches")

    def _build_status_window(self) -> ConditionalContainer:
        return ConditionalContainer(
            Window(content=self.status_control, height=1),
            filter=Condition(lambda: bool(self.status_message)),
        )

    def _build_error_window(self) -> ConditionalContainer:
        return ConditionalContainer(
            Window(content=self.error_control, height=1),
            filter=Condition(lambda: bool(self.error_message)),
        )

    def _build_root_container(self) -> HSplit:
        return HSplit(
            [
                self.title_window,
                self.summary_window,
                self.search_frame,
                self.matches_frame,
                self.status_window,
                self.error_window,
            ]
        )

    def _instructions(self) -> str:
        if self.loading:
            return "Loading next step..."
        if self.max_selections == 1:
            return "Use Up/Down to move, Space to select, type to search, Enter to confirm, Esc to cancel."
        return (
            "Use Up/Down to move, Space to toggle, type to search, Ctrl-A to select visible, "
            "Ctrl-D to clear visible, Enter to confirm, Esc to cancel."
        )

    def _bindings(self) -> KeyBindings:
        kb = KeyBindings()
        active_filter = Condition(lambda: self._is_active() and not self.loading)
        cancel_filter = Condition(self._is_active)

        @kb.add("down", filter=active_filter)
        def _down(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(1)
            event.app.invalidate()

        @kb.add("up", filter=active_filter)
        def _up(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(-1)
            event.app.invalidate()

        @kb.add("pagedown", filter=active_filter)
        def _page_down(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(10)
            event.app.invalidate()

        @kb.add("pageup", filter=active_filter)
        def _page_up(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(-10)
            event.app.invalidate()

        @kb.add(" ", filter=active_filter)
        def _toggle(event: Any) -> None:
            if event.app.layout.has_focus(self.list_window):
                self._toggle_current()
                event.app.invalidate()
                return
            self.search.buffer.insert_text(" ")

        @kb.add("tab", filter=active_filter)
        def _toggle_focus(event: Any) -> None:
            if event.app.layout.has_focus(self.search):
                event.app.layout.focus(self.list_window)
            else:
                event.app.layout.focus(self.search)

        @kb.add("c-a", filter=active_filter)
        def _select_all_visible(event: Any) -> None:
            if event.app.layout.has_focus(self.list_window):
                self._select_visible()
                event.app.invalidate()

        @kb.add("c-d", filter=active_filter)
        def _clear_all_visible(event: Any) -> None:
            if event.app.layout.has_focus(self.list_window):
                self._clear_visible()
                event.app.invalidate()

        @kb.add("enter", filter=active_filter)
        def _submit(event: Any) -> None:
            values = self._selected_values()
            if self.required and not values:
                self.error_message = "Select at least one item."
                event.app.invalidate()
                return
            self.error_message = ""
            if self._on_submit is not None:
                self._on_submit(values)
                event.app.invalidate()
                return
            event.app.exit(result=values)

        @kb.add("escape", filter=cancel_filter)
        @kb.add("c-c", filter=cancel_filter)
        def _cancel(event: Any) -> None:
            if self._on_cancel is not None:
                self._on_cancel()
                event.app.invalidate()
                return
            event.app.exit(result=None)

        return kb

    def _filtered_indexes(self) -> list[int]:
        query = normalize_label(self.search.text)
        if not query:
            return list(range(len(self.choices)))
        tokens = query.split()
        filtered: list[int] = []
        for index, choice in enumerate(self.choices):
            haystack = normalize_label(choice.name)
            if all(token in haystack for token in tokens):
                filtered.append(index)
        return filtered

    def _reset_cursor(self, _event: Any) -> None:
        filtered = self._filtered_indexes()
        self.cursor = 0 if filtered else -1

    def _move(self, amount: int) -> None:
        if self.loading:
            return
        filtered = self._filtered_indexes()
        if not filtered:
            self.cursor = -1
            return
        if self.cursor < 0:
            self.cursor = 0
            return
        self.cursor = max(0, min(self.cursor + amount, len(filtered) - 1))

    def _toggle_current(self) -> None:
        if self.loading:
            return
        filtered = self._filtered_indexes()
        if not filtered or self.cursor < 0:
            return
        choice = self.choices[filtered[self.cursor]]
        if choice.value in self.selected:
            self.selected.remove(choice.value)
            return
        if self.max_selections == 1:
            self.selected = {choice.value}
            return
        self.selected.add(choice.value)

    def _selected_values(self) -> list[str]:
        return [choice.value for choice in self.choices if choice.value in self.selected]

    def _select_visible(self) -> None:
        if self.loading or self.max_selections == 1:
            return
        for choice_index in self._filtered_indexes():
            self.selected.add(self.choices[choice_index].value)

    def _clear_visible(self) -> None:
        if self.loading or self.max_selections == 1:
            return
        for choice_index in self._filtered_indexes():
            self.selected.discard(self.choices[choice_index].value)

    def _choice_style(self, visible_index: int, checked: bool) -> str:
        if visible_index == self.cursor:
            return "class:checked-active" if checked else "class:active"
        if checked:
            return "class:checked"
        return ""

    def _detail_style(self, visible_index: int, checked: bool) -> str:
        if visible_index == self.cursor:
            return "class:checked-detail-active" if checked else "class:detail-active"
        if checked:
            return "class:checked-detail"
        return "class:detail"

    def _meta_style(self, visible_index: int, checked: bool) -> str:
        if visible_index == self.cursor:
            return "class:checked-meta-active" if checked else "class:meta-active"
        if checked:
            return "class:checked-meta"
        return "class:meta"

    def _row_mouse_handler(self, visible_index: int) -> Callable[[MouseEvent], object | None]:
        def handler(mouse_event: MouseEvent) -> object | None:
            if self.loading:
                return None

            app = get_app_or_none()
            if app is not None:
                app.layout.focus(self.list_window)

            if mouse_event.event_type == MouseEventType.MOUSE_UP:
                self.cursor = visible_index
                self._toggle_current()
            elif mouse_event.event_type == MouseEventType.SCROLL_DOWN:
                self._move(1)
            elif mouse_event.event_type == MouseEventType.SCROLL_UP:
                self._move(-1)
            else:
                return NotImplemented

            if app is not None:
                app.invalidate()
            return None

        return handler

    def _render_width(self) -> int:
        app = get_app_or_none()
        if app is None:
            return 100
        return app.output.get_size().columns

    def _detail_width(self, filtered_indexes: list[int]) -> int:
        details = [len(self.choices[index].detail) for index in filtered_indexes if self.choices[index].detail]
        if not details:
            return 0
        return min(max(max(details), 14), 24)

    def _meta_width(self, filtered_indexes: list[int]) -> int:
        metas = [len(self.choices[index].meta) for index in filtered_indexes if self.choices[index].meta]
        if not metas:
            return 0
        return min(max(max(metas), 5), 12)

    def _label_width(self, filtered_indexes: list[int]) -> int:
        prefix_width = 4
        meta_width = self._meta_width(filtered_indexes)
        detail_width = self._detail_width(filtered_indexes)
        total_width = max(self._render_width() - 6, 40)
        spacer_width = (2 if meta_width else 0) + (2 if detail_width else 0)
        return max(18, total_width - prefix_width - spacer_width - meta_width - detail_width)

    def _wrap_label_lines(self, label: str, *, width: int) -> list[str]:
        wrapped = wrap(
            label,
            width=width,
            break_long_words=False,
            break_on_hyphens=False,
        )
        return wrapped or [label]

    def _render_title(self) -> list[tuple[str, str]]:
        self._maybe_before_render()
        return [
            ("class:title", self.title),
            ("", f"\n{self._instructions()}"),
        ]

    def _render_summary(self) -> list[tuple[str, str]]:
        self._maybe_before_render()
        return [("class:muted", "\n".join(self.summary_lines))]

    def _render_status(self) -> list[tuple[str, str]]:
        self._maybe_before_render()
        if not self.status_message:
            return []
        if self.loading:
            return [("class:status", f"{self.status_message} {next(self._spinner_symbols)}")]
        return [("class:status", self.status_message)]

    def _render_choices(self) -> list[tuple[str, str] | tuple[str, str, Callable[[MouseEvent], object | None]]]:
        self._maybe_before_render()
        fragments: list[tuple[str, str] | tuple[str, str, Callable[[MouseEvent], object | None]]] = []
        filtered = self._filtered_indexes()
        if not filtered:
            return [("class:muted", "No matches.\n")]

        meta_width = self._meta_width(filtered)
        detail_width = self._detail_width(filtered)
        label_width = self._label_width(filtered)
        for visible_index, choice_index in enumerate(filtered):
            choice = self.choices[choice_index]
            checked = choice.value in self.selected
            prefix = "[x]" if checked else "[ ]"
            style = self._choice_style(visible_index, checked)
            meta_style = self._meta_style(visible_index, checked)
            detail_style = self._detail_style(visible_index, checked)
            handler = self._row_mouse_handler(visible_index)
            label_lines = self._wrap_label_lines(choice.name, width=label_width)
            for line_index, label_line in enumerate(label_lines):
                row_prefix = f"{prefix} " if line_index == 0 else " " * 4
                meta_text = choice.meta.ljust(meta_width) if line_index == 0 and meta_width else ""
                detail_text = choice.detail.ljust(detail_width) if line_index == 0 and detail_width else ""
                if visible_index == self.cursor and line_index == 0:
                    fragments.append(("[SetCursorPosition]", "", handler))
                fragments.append((style, row_prefix, handler))
                fragments.append((style, label_line.ljust(label_width), handler))
                if meta_width:
                    fragments.append(("", "  ", handler))
                    fragments.append((meta_style, meta_text if meta_text else " " * meta_width, handler))
                if detail_width:
                    fragments.append(("", "  ", handler))
                    fragments.append((detail_style, detail_text if detail_text else " " * detail_width, handler))
                fragments.append((style, "\n", handler))
        return fragments

    def _render_error(self) -> list[tuple[str, str]]:
        self._maybe_before_render()
        return [("class:error", self.error_message)]


class TimeRangePrompt:
    def __init__(
        self,
        *,
        before_render: Callable[[], None] | None = None,
        on_submit: Callable[[tuple[str, str]], None] | None = None,
        on_cancel: Callable[[], None] | None = None,
        is_active: Callable[[], bool] | None = None,
    ) -> None:
        self.title = "Select time range"
        self.caption = ""
        self.start_placeholder = ""
        self.end_placeholder = ""
        self.error_message = ""
        self.summary_lines: list[str] = []
        self.status_message = ""
        self.loading = False
        self._before_render = before_render
        self._on_submit = on_submit
        self._on_cancel = on_cancel
        self._is_active = is_active or (lambda: True)
        self._validator: Callable[[str, str], tuple[str, str]] = lambda start, end: (start, end)
        self._spinner_symbols = cycle("|/-\\")
        self._render_hook_active = False

        self.start_input = TextArea(multiline=False, wrap_lines=False)
        self.end_input = TextArea(multiline=False, wrap_lines=False)
        self.summary_control = FormattedTextControl(self._render_summary)
        self.status_control = FormattedTextControl(self._render_status)
        self.error_control = FormattedTextControl(self._render_error)
        self.title_window = Window(content=FormattedTextControl(self._render_title), height=2)
        self.summary_window = ConditionalContainer(
            Window(content=self.summary_control, height=4),
            filter=Condition(lambda: bool(self.summary_lines)),
        )
        self.range_window = self._build_range_window()
        self.status_window = ConditionalContainer(
            Window(content=self.status_control, height=1),
            filter=Condition(lambda: bool(self.status_message)),
        )
        self.error_window = ConditionalContainer(
            Window(content=self.error_control, height=1),
            filter=Condition(lambda: bool(self.error_message)),
        )
        self.bindings = self._bindings()

    def configure_prompt(
        self,
        *,
        title: str,
        start_value: str,
        end_value: str,
        start_placeholder: str,
        end_placeholder: str,
        caption: str,
        validate: Callable[[str, str], tuple[str, str]],
    ) -> None:
        self.title = title
        self.caption = caption
        self.start_placeholder = start_placeholder
        self.end_placeholder = end_placeholder
        self.error_message = ""
        self._validator = validate
        self.start_input.buffer.text = start_value
        self.end_input.buffer.text = end_value

    def set_summary_lines(self, lines: list[str]) -> None:
        self.summary_lines = list(lines)

    def set_status(self, message: str, *, loading: bool) -> None:
        self.status_message = message
        self.loading = loading
        if not loading:
            self.error_message = ""

    def root_container(self) -> HSplit:
        return HSplit(
            [
                self.title_window,
                self.summary_window,
                self.range_window,
                self.status_window,
                self.error_window,
            ]
        )

    def _build_range_window(self) -> Frame:
        return Frame(
            HSplit(
                [
                    VSplit(
                        [
                            Frame(self.start_input, title="From"),
                            Frame(self.end_input, title="To"),
                        ],
                        padding=1,
                    ),
                    Window(
                        content=FormattedTextControl(self._render_caption),
                        height=2,
                    ),
                ]
            ),
            title="Time Range",
        )

    def _bindings(self) -> KeyBindings:
        kb = KeyBindings()
        active_filter = Condition(lambda: self._is_active() and not self.loading)
        cancel_filter = Condition(self._is_active)

        @kb.add("tab", filter=active_filter)
        def _next_field(event: Any) -> None:
            if event.app.layout.has_focus(self.start_input):
                event.app.layout.focus(self.end_input)
            else:
                event.app.layout.focus(self.start_input)

        @kb.add("s-tab", filter=active_filter)
        def _previous_field(event: Any) -> None:
            if event.app.layout.has_focus(self.end_input):
                event.app.layout.focus(self.start_input)
            else:
                event.app.layout.focus(self.end_input)

        @kb.add("enter", filter=active_filter)
        def _submit(event: Any) -> None:
            try:
                start_value, end_value = self._validator(self.start_input.text.strip(), self.end_input.text.strip())
            except ValueError as exc:
                self.error_message = str(exc)
                event.app.invalidate()
                return
            self.error_message = ""
            self.start_input.buffer.text = start_value
            self.end_input.buffer.text = end_value
            if self._on_submit is not None:
                self._on_submit((start_value, end_value))
                event.app.invalidate()
                return
            event.app.exit(result=(start_value, end_value))

        @kb.add("escape", filter=cancel_filter)
        @kb.add("c-c", filter=cancel_filter)
        def _cancel(event: Any) -> None:
            if self._on_cancel is not None:
                self._on_cancel()
                event.app.invalidate()
                return
            event.app.exit(result=None)

        return kb

    def _maybe_before_render(self) -> None:
        if self._before_render is None or self._render_hook_active:
            return
        self._render_hook_active = True
        try:
            self._before_render()
        finally:
            self._render_hook_active = False

    def _render_title(self) -> list[tuple[str, str]]:
        self._maybe_before_render()
        return [
            ("class:title", self.title),
            ("", "\nEnter From and To, press Enter to confirm, Tab to switch fields, Esc to cancel."),
        ]

    def _render_caption(self) -> list[tuple[str, str]]:
        self._maybe_before_render()
        placeholder_text = f"From: {self.start_placeholder}   To: {self.end_placeholder}"
        content = placeholder_text if not self.caption else f"{self.caption}\n{placeholder_text}"
        return [("class:muted", content)]

    def _render_summary(self) -> list[tuple[str, str]]:
        self._maybe_before_render()
        return [("class:muted", "\n".join(self.summary_lines))]

    def _render_status(self) -> list[tuple[str, str]]:
        self._maybe_before_render()
        if not self.status_message:
            return []
        if self.loading:
            return [("class:status", f"{self.status_message} {next(self._spinner_symbols)}")]
        return [("class:status", self.status_message)]

    def _render_error(self) -> list[tuple[str, str]]:
        self._maybe_before_render()
        return [("class:error", self.error_message)]


class _InteractiveTuiSession:
    def __init__(self) -> None:
        self._queue: Queue[tuple[str, Any]] = Queue()
        self._ready = Event()
        self._closed = Event()
        self._application: Application | None = None
        self._active_request: _PromptRequest | _RangePromptRequest | None = None
        self._active_view = "select"
        self._selector = SearchableMultiSelect(
            title="Loading IMF WEO...",
            choices=[],
            required=False,
            before_render=self._drain_queue,
            on_submit=self._submit_choice_request,
            on_cancel=self._cancel_request,
            is_active=lambda: self._active_view == "select",
        )
        self._range_prompt = TimeRangePrompt(
            before_render=self._drain_queue,
            on_submit=self._submit_range_request,
            on_cancel=self._cancel_request,
            is_active=lambda: self._active_view == "range",
        )
        self._selector_container = ConditionalContainer(
            self._selector._build_root_container(),
            filter=Condition(lambda: self._active_view == "select"),
        )
        self._range_container = ConditionalContainer(
            self._range_prompt.root_container(),
            filter=Condition(lambda: self._active_view == "range"),
        )
        self._thread = Thread(target=self._run_application, daemon=True)

    def start(self) -> None:
        self._thread.start()
        self._ready.wait(timeout=5)

    def close(self) -> None:
        self._queue.put(("close", None))
        self._invalidate()
        self._closed.wait(timeout=5)
        self._thread.join(timeout=1)

    def prompt(
        self,
        *,
        title: str,
        choices: list[Choice],
        required: bool,
        max_selections: int | None,
    ) -> list[str]:
        request = _PromptRequest(
            title=title,
            choices=choices,
            required=required,
            max_selections=max_selections,
            done=Event(),
        )
        self._queue.put(("prompt", request))
        self._invalidate()
        request.done.wait()
        if request.error is not None:
            raise request.error
        return request.result or []

    def prompt_time_range(
        self,
        *,
        title: str,
        start_value: str,
        end_value: str,
        start_placeholder: str,
        end_placeholder: str,
        caption: str,
        validate: Callable[[str, str], tuple[str, str]],
    ) -> tuple[str, str]:
        request = _RangePromptRequest(
            title=title,
            start_value=start_value,
            end_value=end_value,
            start_placeholder=start_placeholder,
            end_placeholder=end_placeholder,
            caption=caption,
            validate=validate,
            done=Event(),
        )
        self._queue.put(("range", request))
        self._invalidate()
        request.done.wait()
        if request.error is not None:
            raise request.error
        if request.result is None:
            raise KeyboardInterrupt("Selection cancelled.")
        return request.result

    def run_task(self, message: str, func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        self._queue.put(("status", (message, True)))
        self._invalidate()
        try:
            return func(*args, **kwargs)
        finally:
            self._queue.put(("status", ("", False)))
            self._invalidate()

    def update_summary(self, lines: list[str]) -> None:
        self._queue.put(("summary", list(lines)))
        self._invalidate()

    def _run_application(self) -> None:
        self._application = Application(
            layout=Layout(HSplit([self._selector_container, self._range_container]), focused_element=self._selector.search),
            key_bindings=merge_key_bindings([self._selector.bindings, self._range_prompt.bindings]),
            full_screen=True,
            mouse_support=True,
            style=self._selector.style,
            refresh_interval=0.1,
        )
        self._ready.set()
        try:
            self._application.run()
        finally:
            self._closed.set()

    def _invalidate(self) -> None:
        if self._application is not None:
            self._application.invalidate()

    def _drain_queue(self) -> None:
        while True:
            try:
                action, payload = self._queue.get_nowait()
            except Empty:
                return
            if action == "prompt":
                request = payload
                assert isinstance(request, _PromptRequest)
                self._active_request = request
                self._active_view = "select"
                self._selector.configure_prompt(
                    title=request.title,
                    choices=request.choices,
                    required=request.required,
                    max_selections=request.max_selections,
                )
                self._selector.set_status("", loading=False)
                if self._application is not None:
                    self._application.layout.focus(self._selector.search)
            elif action == "range":
                request = payload
                assert isinstance(request, _RangePromptRequest)
                self._active_request = request
                self._active_view = "range"
                self._range_prompt.configure_prompt(
                    title=request.title,
                    start_value=request.start_value,
                    end_value=request.end_value,
                    start_placeholder=request.start_placeholder,
                    end_placeholder=request.end_placeholder,
                    caption=request.caption,
                    validate=request.validate,
                )
                self._range_prompt.set_status("", loading=False)
                if self._application is not None:
                    self._application.layout.focus(self._range_prompt.start_input)
            elif action == "summary":
                self._selector.set_summary_lines(payload)
                self._range_prompt.set_summary_lines(payload)
            elif action == "status":
                message, loading = payload
                self._selector.set_status(message, loading=loading)
                self._range_prompt.set_status(message, loading=loading)
            elif action == "close":
                if self._active_request is not None and not self._active_request.done.is_set():
                    self._active_request.error = KeyboardInterrupt("Selection cancelled.")
                    self._active_request.done.set()
                    self._active_request = None
                if self._application is not None:
                    self._application.exit(result=None)
                return

    def _submit_choice_request(self, values: list[str]) -> None:
        request = self._active_request
        if not isinstance(request, _PromptRequest):
            return
        request.result = list(values)
        request.done.set()
        self._active_request = None

    def _submit_range_request(self, values: tuple[str, str]) -> None:
        request = self._active_request
        if not isinstance(request, _RangePromptRequest):
            return
        request.result = tuple(values)
        request.done.set()
        self._active_request = None

    def _cancel_request(self) -> None:
        request = self._active_request
        if request is None:
            if self._application is not None:
                self._application.exit(result=None)
            return
        request.error = KeyboardInterrupt("Selection cancelled.")
        request.done.set()
        self._active_request = None


def _active_session() -> _InteractiveTuiSession | None:
    return _ACTIVE_SESSION


@contextmanager
def interactive_tui_session() -> Any:
    global _ACTIVE_SESSION
    if _ACTIVE_SESSION is not None:
        yield _ACTIVE_SESSION
        return

    session = _InteractiveTuiSession()
    _ACTIVE_SESSION = session
    session.start()
    try:
        yield session
    finally:
        try:
            session.close()
        finally:
            _ACTIVE_SESSION = None


def set_interactive_summary(lines: list[str]) -> None:
    session = _active_session()
    if session is not None:
        session.update_summary(lines)


def prompt_for_choices(title: str, raw_choices: list[dict[str, Any]], required: bool = True) -> list[str]:
    choices = [
        Choice(
            name=str(item["name"]),
            value=str(item["value"]),
            checked=bool(item.get("checked", False)),
            meta=str(item.get("meta", "")),
            detail=str(item.get("detail", "")),
        )
        for item in raw_choices
    ]
    session = _active_session()
    if session is not None:
        return session.prompt(title=title, choices=choices, required=required, max_selections=None)
    return SearchableMultiSelect(title=title, choices=choices, required=required).run()


def prompt_for_choice(title: str, raw_choices: list[dict[str, Any]]) -> str:
    choices = [
        Choice(
            name=str(item["name"]),
            value=str(item["value"]),
            meta=str(item.get("meta", "")),
            detail=str(item.get("detail", "")),
        )
        for item in raw_choices
    ]
    session = _active_session()
    if session is not None:
        result = session.prompt(title=title, choices=choices, required=True, max_selections=1)
    else:
        result = SearchableMultiSelect(title=title, choices=choices, required=True, max_selections=1).run()
    return result[0]


def prompt_for_time_range(
    *,
    title: str,
    start_value: str,
    end_value: str,
    start_placeholder: str,
    end_placeholder: str,
    caption: str,
    validate: Callable[[str, str], tuple[str, str]],
) -> tuple[str, str]:
    session = _active_session()
    if session is not None:
        return session.prompt_time_range(
            title=title,
            start_value=start_value,
            end_value=end_value,
            start_placeholder=start_placeholder,
            end_placeholder=end_placeholder,
            caption=caption,
            validate=validate,
        )

    prompt = TimeRangePrompt()
    prompt.configure_prompt(
        title=title,
        start_value=start_value,
        end_value=end_value,
        start_placeholder=start_placeholder,
        end_placeholder=end_placeholder,
        caption=caption,
        validate=validate,
    )
    application = Application(
        layout=Layout(prompt.root_container(), focused_element=prompt.start_input),
        key_bindings=prompt.bindings,
        full_screen=True,
        mouse_support=True,
        style=SearchableMultiSelect(title=title, choices=[]).style,
        refresh_interval=0.1,
    )
    result = application.run()
    if result is None:
        raise KeyboardInterrupt("Selection cancelled.")
    return result


def run_with_status(message: str, func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    session = _active_session()
    if session is not None:
        return session.run_task(message, func, *args, **kwargs)

    stop = Event()

    def spinner() -> None:
        symbols = cycle("|/-\\")
        while not stop.is_set():
            sys.stderr.write(f"\r{message} {next(symbols)}")
            sys.stderr.flush()
            sleep(0.1)
        sys.stderr.write(f"\r{message} done\n")
        sys.stderr.flush()

    thread = Thread(target=spinner, daemon=True)
    thread.start()
    try:
        return func(*args, **kwargs)
    finally:
        stop.set()
        thread.join(timeout=1)
