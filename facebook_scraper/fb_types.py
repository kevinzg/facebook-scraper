from typing import Any, Callable, Dict, Iterable, Set

from requests import Response
from requests_html import Element


URL = str
Options = Set[str]
Post = Dict[str, Any]
RequestFunction = Callable[[URL], Response]
Element = Element
Page = Iterable[Element]
