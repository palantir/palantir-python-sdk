import itertools
from typing import Any, Callable, Generic, Iterable, List, Optional, TypeVar

T = TypeVar("T")


class Page(Generic[T]):
    """A generic class for iterating over a single page in a paged response."""

    def __init__(self, data: List[T], next_page_token: Optional[str]) -> None:
        self._data = data
        self._next_page_token = next_page_token

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        return self._data.__getitem__(item)

    def __eq__(self, other):
        return self is other or (
            type(self) == type(other) and self._data == other.data and self._next_page_token == other.next_page_token
        )

    @property
    def next_page_token(self):
        return self._next_page_token

    @property
    def data(self):
        return self._data

    def __repr__(self):
        if self._data:
            data_type = self._data[0].__class__.__name__
            return f"Page(data: List[{data_type}], count: {len(self._data)}, next_page_token: {self._next_page_token}])"
        return f"Page(data: None, next_page_token: {self._next_page_token}])"


class PagedResponse(Generic[T]):
    """A generic class for iterating over a paged response.

    By default, the `PagedResponse` acts as a generator over individual
    elements in the response. To iterate over pages, use the generator
    returned by `PagedResponse.paged()`.
    """

    def __init__(self, paged_func: Callable, paged_type: T, response_obj: Any, **kwargs) -> None:
        assert hasattr(response_obj, "next_page_token"), f"Invalid response type {type(response_obj)} cannot be paged"
        self._response_obj = response_obj
        self._next_page_func = paged_func
        self._kwargs = kwargs
        self._next_page_token = response_obj.next_page_token
        self._paged_type = paged_type

    def __repr__(self):
        return f"<iterator[{self._paged_type.__name__}] object foundry.helpers.PagedResponse at {hex(id(self))}>"

    def pages(self) -> Iterable[Page[T]]:
        """Returns a generator over individual pages in a paged response,
        rather than single elements."""
        if self._response_obj is None:
            raise ValueError("Cannot switch iteration mode to pages once iteration has already started")

        # yield the first page we already had before calling pages()
        page = Page(self._response_obj.data, self._response_obj.next_page_token)
        self._response_obj = None
        yield page

        # iterate over following pages
        while self._next_page_token is not None:
            self._kwargs["page_token"] = self._next_page_token
            next_response_obj = self._next_page_func(**self._kwargs)._response_obj
            self._next_page_token = next_response_obj.next_page_token
            yield Page(next_response_obj.data, next_response_obj.next_page_token)

    def __iter__(self):
        return itertools.chain.from_iterable(self.pages())
