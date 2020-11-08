from typing import Any, Callable, Dict, Iterable, Set, Tuple

from requests import Response
from requests_html import Element


URL = str
Options = Dict[str, Any]
Post = Dict[str, Any]
RequestFunction = Callable[[URL], Response]
RawPage = Element
RawPost = Element
Page = Iterable[RawPost]
Credentials = Tuple[str, str]
