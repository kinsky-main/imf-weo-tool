from __future__ import annotations

from dataclasses import dataclass
from itertools import cycle
import sys
from threading import Event, Thread
from time import sleep
from typing import Any, Callable, TypeVar

from prompt_toolkit.application import Application
from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import Dimension, HSplit, Layout, Window
from prompt_toolkit.layout.containers import ConditionalContainer
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import Frame, TextArea

from .legacy import normalize_label


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Choice:
    name: str
    value: str
    checked: bool = False


class SearchableMultiSelect:
    def __init__(self, title: str, choices: list[Choice], required: bool = True) -> None:
        self.title = title
        self.choices = choices
        self.required = required
        self.selected = {choice.value for choice in choices if choice.checked}
        self.cursor = 0
        self.error_message = ""

        self.search = TextArea(prompt="Search: ", multiline=False, wrap_lines=False)
        self.search.buffer.on_text_changed += self._reset_cursor
        self.list_control = FormattedTextControl(self._render_choices, focusable=True)
        self.list_window = Window(
            content=self.list_control,
            wrap_lines=False,
            always_hide_cursor=True,
            height=Dimension(min=10, max=18),
        )
        self.error_control = FormattedTextControl(self._render_error)
        self.style = Style.from_dict(
            {
                "title": "bold",
                "selected": "reverse",
                "muted": "italic",
                "error": "fg:ansired",
            }
        )

    def run(self) -> list[str]:
        bindings = self._bindings()
        root = HSplit(
            [
                Window(
                    content=FormattedTextControl(
                        [
                            ("class:title", self.title),
                            ("", "\nUse Up/Down to move, Space to toggle, type to search, Enter to confirm, Esc to cancel."),
                        ]
                    ),
                    height=2,
                ),
                Frame(self.search),
                Frame(self.list_window, title="Matches"),
                ConditionalContainer(
                    Window(content=self.error_control, height=1),
                    filter=Condition(lambda: bool(self.error_message)),
                ),
            ]
        )
        application = Application(
            layout=Layout(root, focused_element=self.search),
            key_bindings=bindings,
            full_screen=True,
            mouse_support=False,
            style=self.style,
        )
        result = application.run()
        if result is None:
            raise KeyboardInterrupt("Selection cancelled.")
        return result

    def _bindings(self) -> KeyBindings:
        kb = KeyBindings()

        @kb.add("down")
        def _down(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(1)
            event.app.invalidate()

        @kb.add("up")
        def _up(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(-1)
            event.app.invalidate()

        @kb.add("pagedown")
        def _page_down(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(10)
            event.app.invalidate()

        @kb.add("pageup")
        def _page_up(event: Any) -> None:
            event.app.layout.focus(self.list_window)
            self._move(-10)
            event.app.invalidate()

        @kb.add(" ")
        def _toggle(event: Any) -> None:
            if event.app.layout.has_focus(self.list_window):
                self._toggle_current()
                event.app.invalidate()
                return
            self.search.buffer.insert_text(" ")

        @kb.add("tab")
        def _toggle_focus(event: Any) -> None:
            if event.app.layout.has_focus(self.search):
                event.app.layout.focus(self.list_window)
            else:
                event.app.layout.focus(self.search)

        @kb.add("enter")
        def _submit(event: Any) -> None:
            values = self._selected_values()
            if self.required and not values:
                self.error_message = "Select at least one item."
                event.app.invalidate()
                return
            self.error_message = ""
            event.app.exit(result=values)

        @kb.add("escape")
        @kb.add("c-c")
        def _cancel(event: Any) -> None:
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
        filtered = self._filtered_indexes()
        if not filtered:
            self.cursor = -1
            return
        if self.cursor < 0:
            self.cursor = 0
            return
        self.cursor = max(0, min(self.cursor + amount, len(filtered) - 1))

    def _toggle_current(self) -> None:
        filtered = self._filtered_indexes()
        if not filtered or self.cursor < 0:
            return
        choice = self.choices[filtered[self.cursor]]
        if choice.value in self.selected:
            self.selected.remove(choice.value)
        else:
            self.selected.add(choice.value)

    def _selected_values(self) -> list[str]:
        return [choice.value for choice in self.choices if choice.value in self.selected]

    def _render_choices(self) -> list[tuple[str, str]]:
        fragments: list[tuple[str, str]] = []
        filtered = self._filtered_indexes()
        if not filtered:
            return [("class:muted", "No matches.\n")]

        for visible_index, choice_index in enumerate(filtered):
            choice = self.choices[choice_index]
            prefix = "[x]" if choice.value in self.selected else "[ ]"
            style = "class:selected" if visible_index == self.cursor else ""
            fragments.append((style, f"{prefix} {choice.name}\n"))
        return fragments

    def _render_error(self) -> list[tuple[str, str]]:
        return [("class:error", self.error_message)]


def prompt_for_choices(title: str, raw_choices: list[dict[str, Any]], required: bool = True) -> list[str]:
    choices = [
        Choice(name=str(item["name"]), value=str(item["value"]), checked=bool(item.get("checked", False)))
        for item in raw_choices
    ]
    return SearchableMultiSelect(title=title, choices=choices, required=required).run()


def run_with_status(message: str, func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
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
