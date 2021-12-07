from __future__ import annotations

from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    overload,
)

TK = TypeVar("TK")
TV = TypeVar("TV")


class FrozenDict(Mapping[TK, TV]):
    """
    Heavily based on
    https://stackoverflow.com/questions/2703599/what-would-a-frozen-dict

    Ideally replace with https://www.python.org/dev/peps/pep-0603/ if that ever makes it
    in
    """

    # Can't say I understand all of these overloads,
    # but they're copied from Dict and work great!
    @overload
    def __init__(self: FrozenDict[TK, TV]) -> None:
        ...

    @overload
    def __init__(self: FrozenDict[str, TV], **kwargs: TV) -> None:
        ...

    @overload
    def __init__(self, __map: Mapping[TK, TV], **kwargs: TV) -> None:
        ...

    @overload
    def __init__(self, __iterable: Iterable[Tuple[TK, TV]], **kwargs: TV) -> None:
        ...

    @overload
    def __init__(self: FrozenDict[str, str], __iterable: Iterable[list[str]]) -> None:
        ...

    def __init__(self, *args: Any, **kwargs: Any):
        self._d: Dict[TK, TV] = dict(*args, **kwargs)
        self._hash: Optional[int] = None

    def __iter__(self) -> Iterator[TK]:
        return iter(self._d)

    def __len__(self) -> int:
        return len(self._d)

    def __getitem__(self, key: TK) -> TV:
        return self._d[key]

    def __hash__(self) -> int:
        # It would have been simpler and maybe more obvious to use
        # hash(tuple(sorted(self._d.iteritems()))) from this discussion so far, but this
        # solution is O(n). I don't know what kind of n we are going to run into, but
        # sometimes it's hard to resist the urge to optimize when it will gain improved
        # algorithmic performance.
        if self._hash is None:
            hash_ = 0
            for pair in self.items():
                hash_ ^= hash(pair)
            self._hash = hash_
        return self._hash

    def __repr__(self) -> str:
        return "FrozenDict" + self._d.__repr__()

    def as_mutable(self) -> Dict[TK, TV]:
        return self._d.copy()


# TODO we should probably restrict these (as well as other places where we accept
#  FrozenDict) to only take types that can be serialized in protobuf
class TopicName(FrozenDict[str, Any]):
    def as_file_name(self) -> str:
        result = []
        parts = set()
        i = 0
        while f"part{i}" in self:
            result.append(self[f"part{i}"])
            parts.add(f"part{i}")
            i += 1
        for key, value in self.items():
            if key not in parts:
                result.append(f"{key}_{value}")
        return ".".join(result)


# A bit of a hack; a placeholder to use in StatePredicates to represent the current job
CURRENT_JOB = TopicName(__meadowflow_internal__="CURRENT_JOB")


def pname(s: str, /, **kv: Any) -> TopicName:
    """
    "pname" is short for "parse_name"--breaking the naming "rules" here as this function
    will be called "annoyingly often", so we want to make it as short as possible,
    and also don't want to take the commonly used "name".

    This is human-friendly function for parsing a particular style of strings into
    FrozenDicts that are meant to be used as topic/job names.

    s will be parsed into key-value pairs are separated by `/`. Initial key-value pairs
    do not need to explicitly define the key--they will be implicitly given part{i}
    keys. E.g. foo/bar/baz=qux will be turned into {"part0": "foo", "part1": "bar",
    "baz": "qux"}.

    s only supports string values, so non-string values need to be provided as keyword
    arguments (or as **dict), e.g. name("foo/bar", date=datetime.date.today())
    """
    key_values = {}
    part_mode = True
    for i, part in enumerate(s.split("/")):
        if "=" not in part:
            if not part_mode:
                raise ValueError(
                    "Cannot have an un-named part after a named part. E.g. "
                    "foo/bar=baz/qux cannot be parsed--qux is an un-named part after "
                    "the bar=baz named part"
                )
            key_values[f"part{i}"] = part
        else:
            part_mode = False
            i = part.find("=")
            key = part[:i]
            if key in key_values:
                raise ValueError(f"Cannot have multiple parts with the same name {key}")
            key_values[key] = part[i + 1 :]

    for key, value in kv.items():
        if key in key_values:
            raise ValueError(f"Cannot have multiple parts with the same name {key}")
        key_values[key] = value

    return TopicName(key_values)
