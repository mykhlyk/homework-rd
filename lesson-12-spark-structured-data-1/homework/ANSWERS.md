# ANSWERS.md — бонус L12: `build_summary` за один прохід

Дві реалізації одного й того самого mart-а `summary` (25 102 рядки):

| Функція в `job.py` | Ідея |
|---|---|
| `build_summary` | по `summary_slice` на кожен вимір → `unionByName` |
| `build_summary_one_pass` | масив пар `(dimension, dimension_value)` → `explode` → **один** `groupBy` |

Відтворити цифри нижче:

```bash
uv run python bonus_explain.py
```

Скрипт друкує к-сть рядків, кількість `Exchange` і час `.count()` для обох
варіантів, а повні плани зберігає у `plan_union.txt` та `plan_onepass.txt`.

---

## Чому `unionByName` сканує `events` чотири рази

`build_summary` будує окремий піддерево-план на кожен вимір зі
`SUMMARY_DIMENSIONS` (їх 4). Кожен `summary_slice` — це самостійний
`groupBy(dimension).agg(count(*), countDistinct(repo_name))`. `unionByName`
лише склеює чотири готові гілки (union — narrow-операція, **не** shuffle), тому в
підсумковому плані чотири незалежні агрегації, кожна читає `events` (з кешу) з
нуля і кожна робить власний shuffle.

> **Про підрахунок `Exchange`.** `bonus_explain.py` рахує входження підрядка
> `Exchange` у `explain("formatted")`. Formatted-план згадує кожен вузол ~двічі
> (рядок у дереві + окремий блок деталей), тому абсолютні числа приблизно вдвічі
> більші за кількість фізичних `Exchange`-вузлів. Обидва варіанти міряються
> однаково, тож показовим є **відношення**, а не абсолют.

Один `count(distinct …)` у групуванні Spark планує у **дві** фази обміну:

```
HashAggregate(keys=[dimension], functions=[count(*), count(distinct repo_name)])
+- Exchange hashpartitioning(dimension)                     ← 2-й shuffle
   +- HashAggregate(keys=[dimension], partial distinct)
      +- HashAggregate(keys=[dimension, repo_name])
         +- Exchange hashpartitioning(dimension, repo_name) ← 1-й shuffle
            +- HashAggregate(keys=[dimension, repo_name], partial)
               +- <scan events з кешу>
```

Тобто **≈2 `Exchange` на кожен вимір × 4 виміри ≈ 8 `Exchange`** у плані
`build_summary`.

## Чому `explode` робить це за один прохід

`build_summary_one_pass` спершу будує на кожен рядок масив із 4 структур
`(dimension, dimension_value)` і розкриває його `explode`-ом. `explode` —
**narrow**-трансформація: жодного shuffle, просто множення рядків 4× локально в
партиції. Далі — **один** `groupBy(dimension, dimension_value)` з тим самим
`count(distinct repo_name)`, тобто одна агрегація на весь набір:

```
HashAggregate(keys=[dimension, dimension_value], functions=[count(*), count(distinct repo_name)])
+- Exchange hashpartitioning(dimension, dimension_value)          ← 2-й shuffle
   +- HashAggregate(...)
      +- HashAggregate(keys=[dimension, dimension_value, repo_name])
         +- Exchange hashpartitioning(dimension, dimension_value, repo_name) ← 1-й shuffle
            +- HashAggregate(...)
               +- Generate explode(...)                           ← narrow
                  +- <scan events з кешу>
```

Отже одна агрегація незалежно від кількості вимірів. Ключова різниця не в самому
`explode`, а в тому, що чотири окремі агрегації схлопуються в одну.

---

## Результати прогону

> `uv run python bonus_explain.py` на цьому наборі (36 000 → 29 750 подій,
> `spark.sql.shuffle.partitions = 4`, `local[*]`, PySpark/Spark 4).

| Варіант | Рядків | `Exchange` у плані | Час `.count()` |
|---|---|---|---|
| `build_summary` (unionByName) | 25 102 | **26** | **0.285 s** |
| `build_summary_one_pass` (explode) | 25 102 | **8** | **0.252 s** |

Обидва дають ідентичний набір рядків — скрипт це перевіряє (`== 25102`).
`explode`-варіант має **≈3× менше `Exchange`** (8 проти 26): чотири незалежні
агрегації union-варіанта замінюються на одну.

## Висновок

`explode`-варіант прибирає більшість агрегаційних shuffle-ів: один `groupBy`
замість чотирьох, звідси падіння `Exchange` з 26 до 8. Ціна — `explode` роздуває
набір у 4× **перед** shuffle, тож у єдину агрегацію заходить більше сирих рядків;
на 4 вимірах і 30 тис. подіях виграш у часі невеликий (0.285 → 0.252 s, ~12 %),
але він зростає лінійно з кількістю вимірів і обсягом даних. На одному датасеті
різниця в плані (кількість `Exchange`) — надійніший показник, ніж абсолютний час,
який шумить на малому локальному прогоні.

Альтернатива без роздування рядків — `groupingSets` (DataFrame API, Spark 4.0+):
той самий один прохід, але Spark сам мультиплексує групування без явного
`explode`. Підходить, коли виміри — прості колонки; наш `explode`-масив
універсальніший (дозволяє довільні вирази у `dimension_value`).
