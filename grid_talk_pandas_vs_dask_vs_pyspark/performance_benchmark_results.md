# Reading CSV with shape (2611892, 23)

## Pandas without pyarrow:

### Average execution time over 100 executions: 6.231902197599993 seconds

### Memory:

| Field                   | Value             |
|-------------------------|-------------------|
| Index                   | 20895136          |
| Year                    | 20895136          |
| Month                   | 20895136          |
| DayofMonth              | 20895136          |
| DayOfWeek               | 20895136          |
| DepTime                 | 20895136          |
| CRSDepTime              | 20895136          |
| ArrTime                 | 20895136          |
| CRSArrTime              | 20895136          |
| UniqueCarrier           | 154282936         |
| FlightNum               | 20895136          |
| TailNum                 | 123829265         |
| ActualElapsedTime       | 20895136          |
| CRSElapsedTime          | 20895136          |
| AirTime                 | 20895136          |
| ArrDelay                | 20895136          |
| DepDelay                | 20895136          |
| Origin                  | 156713520         |
| Dest                    | 156713520         |
| Distance                | 20895136          |
| TaxiIn                  | 20895136          |
| TaxiOut                 | 20895136          |
| Cancelled               | 20895136          |
| Diverted                | 20895136          |
| **Total memory usage:** | **963 megabytes** |

---

## Pandas with pyarrow:

### Average time: Average execution time over 100 executions: 0.4149747285599733 seconds

### Memory:

| Field                   | Value             |
|-------------------------|-------------------|
| Index                   | 20895136          |
| Year                    | 20895136          |
| Month                   | 20895136          |
| DayofMonth              | 20895136          |
| DayOfWeek               | 20895136          |
| DepTime                 | 21221731          |
| CRSDepTime              | 20895136          |
| ArrTime                 | 21221731          |
| CRSArrTime              | 20895136          |
| UniqueCarrier           | 15852660          |
| FlightNum               | 20895136          |
| TailNum                 | 18388267          |
| ActualElapsedTime       | 21221731          |
| CRSElapsedTime          | 21127517          |
| AirTime                 | 21221681          |
| ArrDelay                | 21221731          |
| DepDelay                | 21221731          |
| Origin                  | 18283244          |
| Dest                    | 18283244          |
| Distance                | 21039865          |
| TaxiIn                  | 21059314          |
| TaxiOut                 | 21059314          |
| Cancelled               | 20895136          |
| Diverted                | 20895136          |
| **Total memory usage:** | **469 megabytes** |

---

## Spark with inferSchema=True

### Average execution time over 100 executions: 0.13562648873994476 seconds

### Memory:

| Field                  | Value            |
|------------------------|------------------|
| **Total memory usage** | **40 megabytes** |

---

## Spark with schema

### Average time: Average execution time over 100 executions: 0.013239120910002384 seconds

### Memory:

| Field                  | Value             |
|------------------------|-------------------|
| **Total memory usage** | **224 megabytes** |

---

## Dask without pyarrow

### Average execution time over 100 executions: 0.01616067666996969 seconds

### Average execution time over 100 executions: 4.680702805580004 seconds (with compute())

### Memory:

| Field                   | Value             |
|-------------------------|-------------------|
| ActualElapsedTime       | 20895136          |
| AirTime                 | 20895136          |
| ArrDelay                | 20895136          |
| ArrTime                 | 20895136          |
| CRSArrTime              | 20895136          |
| CRSDepTime              | 20895136          |
| CRSElapsedTime          | 20895136          |
| Cancelled               | 20895136          |
| DayOfWeek               | 20895136          |
| DayofMonth              | 20895136          |
| DepDelay                | 20895136          |
| DepTime                 | 20895136          |
| Dest                    | 28730812          |
| Distance                | 20895136          |
| Diverted                | 20895136          |
| FlightNum               | 20895136          |
| Index                   | 1280              |
| Month                   | 20895136          |
| Origin                  | 28730812          |
| TailNum                 | 28835835          |
| TaxiIn                  | 20895136          |
| TaxiOut                 | 20895136          |
| UniqueCarrier           | 26300228          |
| Year                    | 20895136          |
| **Total memory usage:** | **486 megabytes** |

---

## Dask with pyarrow

### Average execution time over 100 executions: 0.013629089269961696 seconds

### Average execution time over 100 executions: 1.023525052190016 seconds (with compute())

### Memory:

| Field                   | Value             |
|-------------------------|-------------------|
| ActualElapsedTime       | 21221731          |
| AirTime                 | 21221731          |
| ArrDelay                | 21221731          |
| ArrTime                 | 21221731          |
| CRSArrTime              | 20895136          |
| CRSDepTime              | 20895136          |
| CRSElapsedTime          | 21127517          |
| Cancelled               | 20895136          |
| DayOfWeek               | 20895136          |
| DayofMonth              | 20895136          |
| DepDelay                | 21221731          |
| DepTime                 | 21221731          |
| Dest                    | 18283244          |
| Distance                | 21039865          |
| Diverted                | 20895136          |
| FlightNum               | 20895136          |
| Index                   | 1280              |
| Month                   | 20895136          |
| Origin                  | 18283244          |
| TailNum                 | 28835835          |
| TaxiIn                  | 21059314          |
| TaxiOut                 | 21059314          |
| UniqueCarrier           | 15852660          |
| Year                    | 20895136          |
| **Total memory usage:** | **459 megabytes** |

---

# Filtering a dataset with shape (500000, 10)
