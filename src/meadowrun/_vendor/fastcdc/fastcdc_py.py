# The MIT License (MIT)

# Copyright (c) 2020 Titusz Pan

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# -*- coding: utf-8 -*-
from io import BytesIO
from math import log2


def fastcdc_py(data, min_size=None, avg_size=8192, max_size=None, fat=False, hf=None):
    if min_size is None:
        min_size = avg_size // 4
    if max_size is None:
        max_size = avg_size * 8

    assert MINIMUM_MIN <= min_size <= MINIMUM_MAX
    assert AVERAGE_MIN <= avg_size <= AVERAGE_MAX
    assert MAXIMUM_MIN <= max_size <= MAXIMUM_MAX

    # Ensure we have a readable stream
    if isinstance(data, str):
        stream = open(data, "rb")
    elif not hasattr(data, "read"):
        stream = BytesIO(data)
    else:
        stream = data
    return chunk_generator(stream, min_size, avg_size, max_size, fat, hf)


def chunk_generator(stream, min_size, avg_size, max_size, fat, hf):
    cs = center_size(avg_size, min_size, max_size)
    bits = logarithm2(avg_size)
    mask_s = mask(bits + 1)
    mask_l = mask(bits - 1)

    read_size = max(1024 * 64, max_size)
    blob = memoryview(stream.read(read_size))
    offset = 0
    while blob:
        if len(blob) <= max_size:
            blob = memoryview(bytes(blob) + stream.read(read_size))
        cp = cdc_offset(blob, min_size, avg_size, max_size, cs, mask_s, mask_l)
        chunk = blob[:cp]
        raw = bytes(chunk) if fat else b""
        h = hf(chunk).hexdigest() if hf else ""
        yield Chunk(offset, cp, raw, h)
        offset += cp
        blob = blob[cp:]


def cdc_offset(data, mi, av, ma, cs, mask_s, mask_l):
    pattern = 0
    i = mi
    size = len(data)
    barrier = min(cs, size)
    while i < barrier:
        pattern = (pattern >> 1) + GEAR[data[i]]
        if not pattern & mask_s:
            return i + 1
        i += 1
    barrier = min(ma, size)
    while i < barrier:
        pattern = (pattern >> 1) + GEAR[data[i]]
        if not pattern & mask_l:
            return i + 1
        i += 1
    return i


########################################################################################
# Utility functions and classes                                                        #
########################################################################################


class Chunk:
    __slots__ = ("offset", "length", "data", "hash")

    def __init__(self, offset, length, data, hash):
        self.offset: int = offset
        self.length: int = length
        self.data: bytes = data
        self.hash: str = hash

    def __str__(self):
        return "hash={} offset={} size={}".format(self.hash, self.offset, self.length)


def logarithm2(value):
    return round(log2(value))


def ceil_div(x, y):
    return (x + y - 1) // y


def center_size(average, minimum, source_size):
    offset = minimum + ceil_div(minimum, 2)
    if offset > average:
        offset = average
    size = average - offset
    if size > source_size:
        return source_size
    return size


def mask(bits):
    return 2**bits - 1


########################################################################################
# Constants                                                                            #
########################################################################################


# Smallest acceptable value for the minimum chunk size.
MINIMUM_MIN: int = 64
# Largest acceptable value for the minimum chunk size.
MINIMUM_MAX: int = 67_108_864
# Smallest acceptable value for the average chunk size.
AVERAGE_MIN: int = 256
# Largest acceptable value for the average chunk size.
AVERAGE_MAX: int = 268_435_456
# Smallest acceptable value for the maximum chunk size.
MAXIMUM_MIN: int = 1024
# Largest acceptable value for the maximum chunk size.
MAXIMUM_MAX: int = 1_073_741_824


GEAR = [
    1553318008,
    574654857,
    759734804,
    310648967,
    1393527547,
    1195718329,
    694400241,
    1154184075,
    1319583805,
    1298164590,
    122602963,
    989043992,
    1918895050,
    933636724,
    1369634190,
    1963341198,
    1565176104,
    1296753019,
    1105746212,
    1191982839,
    1195494369,
    29065008,
    1635524067,
    722221599,
    1355059059,
    564669751,
    1620421856,
    1100048288,
    1018120624,
    1087284781,
    1723604070,
    1415454125,
    737834957,
    1854265892,
    1605418437,
    1697446953,
    973791659,
    674750707,
    1669838606,
    320299026,
    1130545851,
    1725494449,
    939321396,
    748475270,
    554975894,
    1651665064,
    1695413559,
    671470969,
    992078781,
    1935142196,
    1062778243,
    1901125066,
    1935811166,
    1644847216,
    744420649,
    2068980838,
    1988851904,
    1263854878,
    1979320293,
    111370182,
    817303588,
    478553825,
    694867320,
    685227566,
    345022554,
    2095989693,
    1770739427,
    165413158,
    1322704750,
    46251975,
    710520147,
    700507188,
    2104251000,
    1350123687,
    1593227923,
    1756802846,
    1179873910,
    1629210470,
    358373501,
    807118919,
    751426983,
    172199468,
    174707988,
    1951167187,
    1328704411,
    2129871494,
    1242495143,
    1793093310,
    1721521010,
    306195915,
    1609230749,
    1992815783,
    1790818204,
    234528824,
    551692332,
    1930351755,
    110996527,
    378457918,
    638641695,
    743517326,
    368806918,
    1583529078,
    1767199029,
    182158924,
    1114175764,
    882553770,
    552467890,
    1366456705,
    934589400,
    1574008098,
    1798094820,
    1548210079,
    821697741,
    601807702,
    332526858,
    1693310695,
    136360183,
    1189114632,
    506273277,
    397438002,
    620771032,
    676183860,
    1747529440,
    909035644,
    142389739,
    1991534368,
    272707803,
    1905681287,
    1210958911,
    596176677,
    1380009185,
    1153270606,
    1150188963,
    1067903737,
    1020928348,
    978324723,
    962376754,
    1368724127,
    1133797255,
    1367747748,
    1458212849,
    537933020,
    1295159285,
    2104731913,
    1647629177,
    1691336604,
    922114202,
    170715530,
    1608833393,
    62657989,
    1140989235,
    381784875,
    928003604,
    449509021,
    1057208185,
    1239816707,
    525522922,
    476962140,
    102897870,
    132620570,
    419788154,
    2095057491,
    1240747817,
    1271689397,
    973007445,
    1380110056,
    1021668229,
    12064370,
    1186917580,
    1017163094,
    597085928,
    2018803520,
    1795688603,
    1722115921,
    2015264326,
    506263638,
    1002517905,
    1229603330,
    1376031959,
    763839898,
    1970623926,
    1109937345,
    524780807,
    1976131071,
    905940439,
    1313298413,
    772929676,
    1578848328,
    1108240025,
    577439381,
    1293318580,
    1512203375,
    371003697,
    308046041,
    320070446,
    1252546340,
    568098497,
    1341794814,
    1922466690,
    480833267,
    1060838440,
    969079660,
    1836468543,
    2049091118,
    2023431210,
    383830867,
    2112679659,
    231203270,
    1551220541,
    1377927987,
    275637462,
    2110145570,
    1700335604,
    738389040,
    1688841319,
    1506456297,
    1243730675,
    258043479,
    599084776,
    41093802,
    792486733,
    1897397356,
    28077829,
    1520357900,
    361516586,
    1119263216,
    209458355,
    45979201,
    363681532,
    477245280,
    2107748241,
    601938891,
    244572459,
    1689418013,
    1141711990,
    1485744349,
    1181066840,
    1950794776,
    410494836,
    1445347454,
    2137242950,
    852679640,
    1014566730,
    1999335993,
    1871390758,
    1736439305,
    231222289,
    603972436,
    783045542,
    370384393,
    184356284,
    709706295,
    1453549767,
    591603172,
    768512391,
    854125182,
]
