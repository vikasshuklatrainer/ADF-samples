"""
=============================================================
  PYTHON BUILT-IN DATA STRUCTURES — COMPLETE DEMONSTRATION
=============================================================
  Structures covered:
    1.  list          — ordered, mutable, allows duplicates
    2.  tuple         — ordered, immutable, allows duplicates
    3.  set           — unordered, mutable, unique elements
    4.  frozenset     — immutable set
    5.  dict          — key-value pairs, ordered (Python 3.7+)
    6.  str           — immutable sequence of characters
    7.  collections.deque        — double-ended queue
    8.  collections.defaultdict  — dict with default values
    9.  collections.OrderedDict  — insertion-ordered dict
   10.  collections.Counter      — frequency map
   11.  collections.namedtuple   — tuple with named fields
   12.  collections.ChainMap     — view over multiple dicts
   13.  heapq                    — min-heap via list
   14.  array.array              — typed numeric array
   15.  Data structure comparison & when to use which
=============================================================
"""

import heapq
import array
from collections import (deque, defaultdict, OrderedDict,
                          Counter, namedtuple, ChainMap)

# ── helpers ───────────────────────────────────────────────────────────────────
def section(n, title):
    print(f"\n{'═'*62}")
    print(f"  {n}. {title}")
    print(f"{'═'*62}")

def h(title):
    print(f"\n  ── {title} {'─'*(50 - len(title))}")

def show(label, value):
    print(f"  {label:<38} {value}")


# ══════════════════════════════════════════════════════════════════════════════
#  1. LIST
#  Ordered · Mutable · Duplicates allowed · O(1) append · O(n) insert/search
# ══════════════════════════════════════════════════════════════════════════════
section(1, "LIST")

h("Creation")
empty       = []
from_range  = list(range(1, 8))
from_str    = list("hello")
nested      = [[1, 2], [3, 4], [5, 6]]
mixed       = [42, "data", 3.14, True, None]

show("empty list         :", empty)
show("from range(1,8)    :", from_range)
show("from string        :", from_str)
show("nested list        :", nested)
show("mixed types        :", mixed)

h("Indexing & Slicing")
fruits = ["apple", "banana", "cherry", "date", "elderberry"]
show("fruits             :", fruits)
show("fruits[0]          :", fruits[0])
show("fruits[-1]         :", fruits[-1])
show("fruits[1:4]        :", fruits[1:4])
show("fruits[::2]        :", fruits[::2])         # every other element
show("fruits[::-1]       :", fruits[::-1])         # reverse

h("Mutation methods")
nums = [3, 1, 4, 1, 5, 9, 2, 6]
show("original           :", nums)

nums.append(7)
show("after .append(7)   :", nums)

nums.insert(2, 99)
show("after .insert(2,99):", nums)

nums.remove(99)
show("after .remove(99)  :", nums)

popped = nums.pop()
show(f"popped={popped}, list   :", nums)

nums.sort()
show("after .sort()      :", nums)

nums.reverse()
show("after .reverse()   :", nums)

nums.extend([10, 11])
show("after .extend(...)  :", nums)

show("nums.count(1)       :", nums.count(1))
show("nums.index(5)       :", nums.index(5))

h("List comprehensions")
squares     = [x**2 for x in range(1, 8)]
even_sq     = [x**2 for x in range(1, 11) if x % 2 == 0]
flat        = [n for row in nested for n in row]
matrix      = [[i * j for j in range(1, 4)] for i in range(1, 4)]

show("squares            :", squares)
show("even squares        :", even_sq)
show("flattened nested    :", flat)
show("3×3 matrix          :", matrix)

h("Useful built-ins on lists")
data = [5, 2, 8, 1, 9, 3]
show("len(data)          :", len(data))
show("min(data)          :", min(data))
show("max(data)          :", max(data))
show("sum(data)          :", sum(data))
show("sorted(data)       :", sorted(data))
show("sorted(desc)       :", sorted(data, reverse=True))
show("enumerate sample   :", list(enumerate(data[:3])))
show("zip sample         :", list(zip(data[:3], squares[:3])))

h("Copying — shallow vs deep")
import copy
original  = [[1, 2], [3, 4]]
shallow   = original.copy()        # outer list copied, inner lists shared
deep      = copy.deepcopy(original)

original[0][0] = 999
show("modified original   :", original)
show("shallow copy (affected):", shallow)   # inner list IS shared
show("deep copy (safe)    :", deep)          # fully independent


# ══════════════════════════════════════════════════════════════════════════════
#  2. TUPLE
#  Ordered · Immutable · Duplicates allowed · Hashable · Slightly faster than list
# ══════════════════════════════════════════════════════════════════════════════
section(2, "TUPLE")

h("Creation")
empty_t      = ()
single       = (42,)               # trailing comma makes it a tuple
coords       = (10.5, 20.3)
rgb          = (255, 128, 0)
nested_t     = ((1, 2), (3, 4))
from_list    = tuple([1, 2, 3])

show("empty tuple        :", empty_t)
show("single element     :", single)
show("coordinates        :", coords)
show("rgb color          :", rgb)
show("nested             :", nested_t)
show("from list          :", from_list)

h("Indexing & Slicing  (same as list)")
point = (3, 7, 12, 4, 9)
show("point              :", point)
show("point[2]           :", point[2])
show("point[-1]          :", point[-1])
show("point[1:4]         :", point[1:4])
show("point[::-1]        :", point[::-1])

h("Immutability  (tuples cannot be changed)")
try:
    coords[0] = 999
except TypeError as e:
    show("Attempt to modify  :", f"TypeError → {e}")

h("Packing & Unpacking")
packed            = 1, 2, 3               # packing (no parentheses needed)
a, b, c           = packed                # unpacking
first, *rest      = (10, 20, 30, 40, 50)  # starred unpacking
*head, last       = (10, 20, 30, 40, 50)

show("packed             :", packed)
show("a, b, c            :", (a, b, c))
show("first, rest        :", (first, rest))
show("head, last         :", (head, last))

# Swap without a temp variable (uses tuple packing/unpacking under the hood)
x, y = 100, 200
x, y = y, x
show("after swap x,y     :", (x, y))

h("Tuples as dict keys  (lists cannot be)")
grid = {(0, 0): "origin", (1, 0): "right", (0, 1): "up"}
show("grid lookup (0,0)  :", grid[(0, 0)])

h("Named fields with zip")
headers = ("name", "age", "city")
values  = ("Alice", 30, "Chicago")
record  = dict(zip(headers, values))
show("record from zip    :", record)

h("tuple methods")
t = (1, 2, 2, 3, 2, 4)
show("t.count(2)         :", t.count(2))
show("t.index(3)         :", t.index(3))


# ══════════════════════════════════════════════════════════════════════════════
#  3. SET
#  Unordered · Mutable · Unique elements · O(1) lookup · Not subscriptable
# ══════════════════════════════════════════════════════════════════════════════
section(3, "SET")

h("Creation")
empty_s   = set()                          # NOT {} — that creates a dict
from_list = set([1, 2, 2, 3, 3, 3, 4])    # duplicates removed automatically
letters   = {'a', 'b', 'c', 'd', 'e'}
from_str  = set("mississippi")             # unique chars only

show("empty set          :", empty_s)
show("from list (deduped):", from_list)
show("letters            :", sorted(letters))
show("unique chars        :", sorted(from_str))

h("Adding & Removing")
s = {1, 2, 3}
s.add(4)
show("after .add(4)      :", s)

s.add(2)                                   # adding existing element — no effect
show("after .add(2) again:", s)

s.discard(10)                              # safe remove — no error if missing
show("after .discard(10) :", s)

s.remove(3)
show("after .remove(3)   :", s)

popped = s.pop()                           # removes and returns an arbitrary element
show(f"popped={popped}, set     :", s)

h("Set operations — the core power of sets")
A = {1, 2, 3, 4, 5}
B = {4, 5, 6, 7, 8}

show("A                  :", A)
show("B                  :", B)
show("A | B  (union)      :", A | B)
show("A & B  (intersect)  :", A & B)
show("A - B  (difference) :", A - B)
show("B - A  (difference) :", B - A)
show("A ^ B  (symmetric △):", A ^ B)       # in A or B but not both

h("Set predicates")
X = {1, 2}
Y = {1, 2, 3, 4}
show("X.issubset(Y)      :", X.issubset(Y))
show("Y.issuperset(X)    :", Y.issuperset(X))
show("X.isdisjoint({5,6}):", X.isdisjoint({5, 6}))

h("Set comprehension")
even_set = {x for x in range(20) if x % 2 == 0}
show("even numbers 0–19  :", sorted(even_set))

h("Common use-case: fast deduplication & membership")
emails_raw   = ["a@x.com", "b@x.com", "a@x.com", "c@x.com", "b@x.com"]
unique_emails = list(set(emails_raw))
show("unique emails       :", sorted(unique_emails))
show('"b@x.com" in set   :', "b@x.com" in set(emails_raw))


# ══════════════════════════════════════════════════════════════════════════════
#  4. FROZENSET
#  Immutable set · Hashable (usable as dict key or set element)
# ══════════════════════════════════════════════════════════════════════════════
section(4, "FROZENSET")

h("Creation")
fs = frozenset([1, 2, 3, 4, 2])
show("frozenset          :", fs)

h("Supports all read-only set operations")
A = frozenset({1, 2, 3})
B = frozenset({3, 4, 5})
show("A | B              :", A | B)
show("A & B              :", A & B)
show("A - B              :", A - B)

h("Immutability")
try:
    fs.add(99)
except AttributeError as e:
    show("Attempt to add     :", f"AttributeError → {e}")

h("Use as dict key  (regular sets cannot)")
permissions = {
    frozenset({"read"}):             "viewer",
    frozenset({"read", "write"}):    "editor",
    frozenset({"read", "write", "admin"}): "owner",
}
role = frozenset({"read", "write"})
show("role lookup        :", permissions[role])


# ══════════════════════════════════════════════════════════════════════════════
#  5. DICT
#  Ordered (3.7+) · Mutable · Key→Value · O(1) average get/set
# ══════════════════════════════════════════════════════════════════════════════
section(5, "DICT")

h("Creation — multiple ways")
empty_d   = {}
from_kw   = dict(name="Alice", age=30, city="Chicago")
from_zip  = dict(zip(["x", "y", "z"], [10, 20, 30]))
from_list = dict([("a", 1), ("b", 2), ("c", 3)])
nested_d  = {"user": {"id": 1, "roles": ["admin", "editor"]}}

show("from keywords      :", from_kw)
show("from zip           :", from_zip)
show("from list of tuples:", from_list)

h("Accessing values")
person = {"name": "Bob", "age": 25, "city": "Houston"}
show('person["name"]     :', person["name"])
show('person.get("age")  :', person.get("age"))
show('missing with default:', person.get("email", "N/A"))  # safe — no KeyError

h("Adding & Updating")
person["email"] = "bob@email.com"
show("after add email    :", person)

person.update({"age": 26, "phone": "555-0100"})
show("after .update()    :", person)

h("Removing")
removed = person.pop("phone")
show(f"popped={removed}, dict:", person)

person.setdefault("country", "USA")   # adds only if key absent
show("after .setdefault():", person)

h("Iteration patterns")
inventory = {"apples": 50, "bananas": 30, "cherries": 200}
print("\n  Keys   :", list(inventory.keys()))
print("  Values :", list(inventory.values()))
print("  Items  :", list(inventory.items()))

print("\n  Iterating items:")
for k, v in inventory.items():
    bar = "█" * (v // 10)
    print(f"    {k:<10} {v:>4}  {bar}")

h("Dict comprehension")
squared = {x: x**2 for x in range(1, 7)}
show("x → x² (1–6)       :", squared)

filtered = {k: v for k, v in inventory.items() if v > 40}
show("stock > 40         :", filtered)

inverted = {v: k for k, v in from_zip.items()}
show("inverted dict      :", inverted)

h("Merging dicts  (Python 3.9+ uses |)")
d1 = {"a": 1, "b": 2}
d2 = {"b": 99, "c": 3}
merged = {**d1, **d2}           # d2 wins on key conflict
show("merged (**unpack)  :", merged)

h("Nested access & dict.get() chaining")
config = {"db": {"host": "localhost", "port": 5432}, "debug": True}
host = config.get("db", {}).get("host", "unknown")
show("nested .get()      :", host)

h("Counting with dict")
words = ["data", "pipeline", "data", "lake", "data", "pipeline"]
freq = {}
for w in words:
    freq[w] = freq.get(w, 0) + 1
show("word frequency dict :", freq)


# ══════════════════════════════════════════════════════════════════════════════
#  6. STRING  (immutable sequence)
# ══════════════════════════════════════════════════════════════════════════════
section(6, "STRING (immutable sequence)")

h("Strings as sequences")
s = "Data Engineering"
show("s[0]               :", s[0])
show("s[-1]              :", s[-1])
show("s[5:16]            :", s[5:16])
show("s[::-1]            :", s[::-1])
show("len(s)             :", len(s))
show('"i" in s           :', "i" in s)

h("Key string methods")
sample = "  Hello, World!  "
show(".strip()           :", sample.strip())
show(".lower()           :", sample.strip().lower())
show(".upper()           :", sample.strip().upper())
show(".replace()         :", sample.strip().replace("World", "Python"))
show(".split(',')        :", sample.strip().split(","))
show(".startswith('He')  :", sample.strip().startswith("He"))
show(".find('World')     :", sample.strip().find("World"))
show(".count('l')        :", sample.strip().count("l"))

csv_line = "Alice,30,Chicago,Premium"
show(".split(',')        :", csv_line.split(","))
show("'-'.join(list)     :", "-".join(["a", "b", "c"]))

h("f-strings and formatting")
name, score = "Alice", 95.678
show("f-string basic     :", f"Name: {name}, Score: {score}")
show("f-string .2f       :", f"Score: {score:.2f}")
show("f-string width     :", f"{name:<10}|{score:>8.1f}")
show("f-string expr      :", f"2 + 2 = {2 + 2}")

h("String ↔ list conversions")
words = "the quick brown fox".split()
show("split → list       :", words)
show("join ← list        :", " ".join(words))
show("sorted words       :", sorted(words))
show("unique chars        :", sorted(set("banana")))


# ══════════════════════════════════════════════════════════════════════════════
#  7. collections.deque
#  Double-ended queue · O(1) append/pop from both ends · Thread-safe
# ══════════════════════════════════════════════════════════════════════════════
section(7, "collections.deque  (double-ended queue)")

h("Creation")
dq = deque([10, 20, 30, 40, 50])
show("initial deque      :", dq)

dq_max = deque(maxlen=4)              # bounded deque — old items auto-evicted
for i in range(1, 7):
    dq_max.append(i)
show("bounded maxlen=4   :", dq_max)  # only last 4 kept

h("Append & pop from BOTH ends  (O(1))")
dq.append(60)
show("appendright(60)    :", dq)

dq.appendleft(0)
show("appendleft(0)      :", dq)

dq.pop()
show("pop() (right)      :", dq)

dq.popleft()
show("popleft()          :", dq)

h("Rotate")
dq2 = deque([1, 2, 3, 4, 5])
dq2.rotate(2)
show("rotate(+2) right   :", dq2)
dq2.rotate(-2)
show("rotate(-2) left    :", dq2)

h("Extend both ends")
dq3 = deque([3, 4, 5])
dq3.extendleft([2, 1, 0])    # note: added one-by-one from left, so order reverses
show("extendleft([2,1,0]):", dq3)

h("Use-case: sliding window (last N log lines)")
log_buffer = deque(maxlen=3)
for msg in ["INFO start", "DEBUG init", "INFO connect", "ERROR fail", "INFO retry"]:
    log_buffer.append(msg)
    print(f"    buffer: {list(log_buffer)}")

h("Use-case: BFS queue")
bfs_queue = deque(["root"])
visited   = []
graph     = {"root": ["A", "B"], "A": ["C"], "B": ["D"], "C": [], "D": []}
while bfs_queue:
    node = bfs_queue.popleft()
    visited.append(node)
    bfs_queue.extend(graph.get(node, []))
show("BFS traversal order:", visited)


# ══════════════════════════════════════════════════════════════════════════════
#  8. collections.defaultdict
#  dict with automatic default value for missing keys
# ══════════════════════════════════════════════════════════════════════════════
section(8, "collections.defaultdict")

h("int default — counting without KeyError")
word_count = defaultdict(int)
for word in "data pipeline data lake data warehouse pipeline".split():
    word_count[word] += 1
show("word counts        :", dict(word_count))
show("missing key → 0    :", word_count["unknown_word"])

h("list default — grouping")
transactions = [
    ("Alice", 120.50), ("Bob", 80.00), ("Alice", 45.99),
    ("Carol", 200.00), ("Bob", 33.50), ("Alice", 67.25),
]
by_customer = defaultdict(list)
for name, amount in transactions:
    by_customer[name].append(amount)

for cust, amounts in by_customer.items():
    total = sum(amounts)
    show(f"  {cust} ({len(amounts)} orders)", f"total=${total:.2f}  amounts={amounts}")

h("set default — unique tags per post")
tags = defaultdict(set)
tagging = [("post1","python"),("post1","data"),("post2","python"),
           ("post1","etl"),  ("post2","cloud")]
for post, tag in tagging:
    tags[post].add(tag)
for post, tag_set in tags.items():
    show(f"  {post} tags         :", sorted(tag_set))

h("dict default — nested structure without KeyError")
nested_dd = defaultdict(lambda: defaultdict(int))
nested_dd["North"]["Q1"] += 500
nested_dd["North"]["Q2"] += 700
nested_dd["South"]["Q1"] += 300
for region, quarters in nested_dd.items():
    show(f"  {region}            :", dict(quarters))


# ══════════════════════════════════════════════════════════════════════════════
#  9. collections.OrderedDict
#  Like dict but with order-aware methods; useful for LRU cache patterns
# ══════════════════════════════════════════════════════════════════════════════
section(9, "collections.OrderedDict")

h("Creation and order preservation")
od = OrderedDict()
od["first"]  = 1
od["second"] = 2
od["third"]  = 3
show("OrderedDict        :", od)

h("move_to_end  (key feature vs regular dict)")
od.move_to_end("first")
show("move_to_end('first'):", od)
od.move_to_end("third", last=False)    # move to front
show("move_to_end front  :", od)

h("popitem — LIFO/FIFO control")
od2 = OrderedDict([("a", 1), ("b", 2), ("c", 3)])
show("popitem(last=True) :", od2.popitem(last=True))    # pop from end
show("popitem(last=False):", od2.popitem(last=False))   # pop from front
show("remaining          :", od2)

h("Use-case: LRU cache simulation")
class LRUCache:
    def __init__(self, capacity):
        self.cap   = capacity
        self.cache = OrderedDict()

    def get(self, key):
        if key not in self.cache:
            return -1
        self.cache.move_to_end(key)   # mark as recently used
        return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.cap:
            self.cache.popitem(last=False)  # evict least recently used

lru = LRUCache(3)
for k, v in [("a",1),("b",2),("c",3),("d",4)]:
    lru.put(k, v)
    show(f"  put({k},{v}) cache   :", dict(lru.cache))

lru.get("b")
show("  get('b') cache   :", dict(lru.cache))   # b moved to end


# ══════════════════════════════════════════════════════════════════════════════
#  10. collections.Counter
#  Subclass of dict optimised for counting hashable objects
# ══════════════════════════════════════════════════════════════════════════════
section(10, "collections.Counter")

h("Creation")
from_list_c  = Counter([1, 2, 2, 3, 3, 3, 4, 4, 4, 4])
from_string  = Counter("abracadabra")
from_dict    = Counter({"red": 4, "blue": 2, "green": 7})
from_kw      = Counter(cats=3, dogs=5, fish=1)

show("from list           :", from_list_c)
show("from string         :", from_string)
show("from dict           :", from_dict)
show("from keywords       :", from_kw)

h("most_common")
text = "the data pipeline processes data and the data lake stores data"
wc   = Counter(text.split())
show("most_common(3)      :", wc.most_common(3))
show("least common 3      :", wc.most_common()[:-4:-1])   # last 3

h("Arithmetic on Counters")
c1 = Counter({"a": 5, "b": 3, "c": 2})
c2 = Counter({"a": 2, "b": 4, "d": 1})
show("c1                  :", c1)
show("c2                  :", c2)
show("c1 + c2  (add)      :", c1 + c2)
show("c1 - c2  (subtract) :", c1 - c2)   # only positive results
show("c1 & c2  (min)      :", c1 & c2)   # intersection
show("c1 | c2  (max)      :", c1 | c2)   # union

h("Update and subtract")
inventory = Counter(apples=10, bananas=5)
sold       = Counter(apples=3, bananas=5, cherries=1)
inventory.subtract(sold)
show("after subtract      :", inventory)
inventory.update({"apples": 7, "cherries": 10})
show("after update        :", inventory)

h("elements() — expand back to list")
c = Counter({"a": 3, "b": 1})
show("elements()          :", sorted(c.elements()))

h("Use-case: vote tally")
votes = ["Alice","Bob","Alice","Carol","Bob","Alice","Carol","Alice"]
tally = Counter(votes)
winner = tally.most_common(1)[0]
show("vote tally          :", tally)
show("winner              :", f"{winner[0]} with {winner[1]} votes")


# ══════════════════════════════════════════════════════════════════════════════
#  11. collections.namedtuple
#  Tuple subclass with named fields · Immutable · Memory efficient
# ══════════════════════════════════════════════════════════════════════════════
section(11, "collections.namedtuple")

h("Defining named tuples")
Point   = namedtuple("Point",   ["x", "y"])
Color   = namedtuple("Color",   ["red", "green", "blue"])
Product = namedtuple("Product", ["id", "name", "price", "category"])

p     = Point(3, 7)
white = Color(255, 255, 255)
item  = Product(101, "Wireless Mouse", 29.99, "Electronics")

show("Point(3,7)         :", p)
show("p.x, p.y           :", (p.x, p.y))
show("Color white        :", white)
show("Product            :", item)
show("item.name          :", item.name)
show("item[2]  (by index):", item[2])

h("Immutability — cannot assign to fields")
try:
    p.x = 99
except AttributeError as e:
    show("p.x = 99           :", f"AttributeError → {e}")

h("_replace  — create modified copy")
updated_price = item._replace(price=24.99)
show("_replace(price)    :", updated_price)

h("_asdict  — convert to OrderedDict")
show("_asdict()          :", item._asdict())

h("_fields  — inspect field names")
show("Product._fields    :", Product._fields)

h("Defaults (Python 3.6.1+)")
Employee = namedtuple("Employee", ["name", "dept", "salary"],
                      defaults=["Engineering", 60_000])
e1 = Employee("Alice")                          # uses both defaults
e2 = Employee("Bob", "Marketing", 75_000)       # overrides both
show("Employee defaults   :", e1)
show("Employee explicit   :", e2)

h("Use-case: CSV rows as named tuples")
import csv, io
csv_data = "id,product,price\n1,Mouse,29.99\n2,Keyboard,79.99"
reader   = csv.DictReader(io.StringIO(csv_data))
Row      = namedtuple("Row", ["id", "product", "price"])
rows     = [Row(r["id"], r["product"], float(r["price"])) for r in reader]
for row in rows:
    show(f"  {row.product:<12} ${row.price:.2f}", f"(id={row.id})")


# ══════════════════════════════════════════════════════════════════════════════
#  12. collections.ChainMap
#  Single view across multiple dicts — reads cascade, writes go to first map
# ══════════════════════════════════════════════════════════════════════════════
section(12, "collections.ChainMap")

h("Layered configuration (env → user → defaults)")
defaults = {"theme": "light", "language": "en", "timeout": 30, "debug": False}
user_cfg = {"theme": "dark", "timeout": 60}
env_vars = {"debug": True}                          # highest priority

config = ChainMap(env_vars, user_cfg, defaults)     # first map wins on conflict
show("config['theme']    :", config["theme"])        # from user_cfg
show("config['debug']    :", config["debug"])        # from env_vars
show("config['language'] :", config["language"])     # from defaults
show("config['timeout']  :", config["timeout"])      # from user_cfg

h("Writes only affect the first map")
config["new_key"] = "new_value"
show("env_vars after write:", env_vars)              # new_key added here only

h("maps attribute — see all layers")
for i, m in enumerate(config.maps):
    show(f"  layer {i}           :", dict(m))

h("new_child  — add a temporary override layer")
temp = config.new_child({"theme": "high-contrast"})
show("temp['theme']      :", temp["theme"])
show("config['theme']    :", config["theme"])        # original unchanged

h("Use-case: variable scoping (local → enclosing → global)")
builtin_scope  = {"len": "built-in", "print": "built-in"}
global_scope   = {"x": 10, "PI": 3.14}
local_scope    = {"x": 99}                          # shadows global x

scope_chain    = ChainMap(local_scope, global_scope, builtin_scope)
show("x (local shadows)  :", scope_chain["x"])       # 99, not 10
show("PI (from global)   :", scope_chain["PI"])
show("len (from builtin) :", scope_chain["len"])


# ══════════════════════════════════════════════════════════════════════════════
#  13. heapq  (min-heap via a plain list)
#  Always O(log n) push/pop · Root is always the smallest element
# ══════════════════════════════════════════════════════════════════════════════
section(13, "heapq  (min-heap)")

h("heapify — convert list to heap in O(n)")
nums = [9, 3, 7, 1, 5, 8, 2, 6, 4]
heapq.heapify(nums)
show("heapified list     :", nums)
show("smallest (root)    :", nums[0])

h("heappush and heappop")
hp = []
for val in [5, 3, 8, 1, 9, 2]:
    heapq.heappush(hp, val)
    show(f"  push({val}) heap    :", hp)

print()
while hp:
    show(f"  pop → {heapq.heappop(hp):<4} heap  :", hp)  # always smallest first

h("nlargest and nsmallest — O(n log k)")
data = [15, 3, 22, 8, 47, 1, 36, 12, 9, 29]
show("nlargest(3)        :", heapq.nlargest(3, data))
show("nsmallest(3)       :", heapq.nsmallest(3, data))

h("Max-heap trick  (negate values)")
max_hp = []
for v in [5, 3, 8, 1, 9, 2]:
    heapq.heappush(max_hp, -v)
show("max-heap (negated) :", max_hp)
show("largest element    :", -heapq.heappop(max_hp))

h("Priority queue with tuples  (priority, item)")
pq = []
tasks = [(3,"low-priority"), (1,"URGENT"), (2,"normal"), (1,"ALSO URGENT")]
for priority, task in tasks:
    heapq.heappush(pq, (priority, task))

print("\n  Processing tasks by priority:")
while pq:
    pri, task = heapq.heappop(pq)
    print(f"    [{pri}] {task}")

h("heapq.merge — merge sorted iterables")
stream1 = [1, 5, 9]
stream2 = [2, 4, 8]
stream3 = [3, 6, 7]
merged  = list(heapq.merge(stream1, stream2, stream3))
show("merged sorted streams:", merged)


# ══════════════════════════════════════════════════════════════════════════════
#  14. array.array  (typed numeric array — more memory-efficient than list)
# ══════════════════════════════════════════════════════════════════════════════
section(14, "array.array  (typed numeric array)")

h("Type codes")
print("  Code  C type        Python type   Size")
print("  ─────────────────────────────────────")
for code, ct, pt, sz in [
    ("b", "signed char",    "int",    1),
    ("i", "signed int",     "int",    2),
    ("l", "signed long",    "int",    4),
    ("f", "float",          "float",  4),
    ("d", "double",         "float",  8),
]:
    print(f"  {code!r}     {ct:<15} {pt:<12} {sz} bytes")

h("Creation")
int_arr   = array.array("i", [1, 2, 3, 4, 5])
float_arr = array.array("f", [1.1, 2.2, 3.3])
show("int array          :", int_arr)
show("float array        :", float_arr)

h("Operations")
int_arr.append(6)
show("after .append(6)   :", int_arr)

int_arr.extend([7, 8, 9])
show("after .extend(...)  :", int_arr)

int_arr.remove(5)
show("after .remove(5)   :", int_arr)

show("int_arr[2]         :", int_arr[2])
show("int_arr[1:4]       :", int_arr[1:4])
show("sum                :", sum(int_arr))

h("Memory comparison — list vs array")
import sys
py_list  = list(range(10_000))
arr_ints = array.array("i", range(10_000))
show("list  (10k ints) bytes:", sys.getsizeof(py_list))
show("array (10k ints) bytes:", arr_ints.buffer_info()[1] * arr_ints.itemsize)
show("array.itemsize     :", arr_ints.itemsize)
show("array.buffer_info():", arr_ints.buffer_info())

h("Type safety")
try:
    int_arr.append(3.14)                   # floats rejected in int array
except TypeError as e:
    show("append float to int :", f"TypeError → {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  15. DATA STRUCTURE COMPARISON — WHEN TO USE WHICH
# ══════════════════════════════════════════════════════════════════════════════
section(15, "COMPARISON — WHEN TO USE WHICH")

print("""
  ┌─────────────────────┬───────────┬───────────┬──────────┬──────────────────────────────────────┐
  │ Structure           │ Ordered   │ Mutable   │ Dupes?   │ Best used for                        │
  ├─────────────────────┼───────────┼───────────┼──────────┼──────────────────────────────────────┤
  │ list                │ ✔         │ ✔         │ ✔        │ General sequence, stack (append/pop) │
  │ tuple               │ ✔         │ ✘         │ ✔        │ Fixed records, dict keys, unpacking  │
  │ set                 │ ✘         │ ✔         │ ✘        │ Membership, dedup, set math          │
  │ frozenset           │ ✘         │ ✘         │ ✘        │ Immutable set, dict key, set element │
  │ dict                │ ✔ (3.7+)  │ ✔         │ keys: ✘  │ Key→value lookup, grouping           │
  │ str                 │ ✔         │ ✘         │ ✔        │ Text processing                      │
  │ deque               │ ✔         │ ✔         │ ✔        │ Queue/stack, sliding window, BFS/DFS │
  │ defaultdict         │ ✔         │ ✔         │ keys: ✘  │ Counting, grouping, avoiding KeyError│
  │ OrderedDict         │ ✔         │ ✔         │ keys: ✘  │ LRU cache, order-aware dict ops      │
  │ Counter             │ ✔         │ ✔         │ keys: ✘  │ Frequency counting, top-N, set math  │
  │ namedtuple          │ ✔         │ ✘         │ ✔        │ Lightweight records, CSV rows        │
  │ ChainMap            │ ✔         │ first map │ keys: ✘  │ Layered config, variable scoping     │
  │ heapq               │ heap order│ ✔         │ ✔        │ Priority queue, top-N, sorted merge  │
  │ array.array         │ ✔         │ ✔         │ ✔        │ Large numeric arrays (memory saving) │
  └─────────────────────┴───────────┴───────────┴──────────┴──────────────────────────────────────┘

  BIG-O CHEAT SHEET
  ─────────────────
  list    : access O(1) · search O(n) · insert-end O(1) · insert-mid O(n)
  tuple   : access O(1) · search O(n)
  set     : add O(1)    · lookup O(1) · delete O(1)
  dict    : get  O(1)   · set    O(1) · delete O(1)
  deque   : appendleft/popleft O(1)   vs list O(n)
  heapq   : push O(log n) · pop O(log n) · peek O(1)
  Counter : most_common(k) O(n log k)

  QUICK DECISION GUIDE
  ────────────────────
  Need fast lookup by key?                        → dict
  Need unique items + set math?                   → set
  Need immutable, hashable container?             → tuple or frozenset
  Need a queue (FIFO) or stack (LIFO)?            → deque
  Counting / top-N frequencies?                   → Counter
  Grouping without key-error risk?                → defaultdict
  Priority ordering?                              → heapq
  Lightweight record with named fields?           → namedtuple
  Layered / fallback config?                      → ChainMap
  Large array of same-type numbers?               → array.array
""")

print("✔  All data structures demonstrated successfully.\n")
