## Pandas without pyarrow:

### Average time: Average execution time over 100 executions: 0.5585623467800178 seconds

### Memory:

| Field                  | Value             |
|------------------------|-------------------|
| Index                  | 128               |
| UserID                 | 4000000           |
| TransactionDate        | 33500000          |
| ProductID              | 4000000           |
| ProductName            | 32100860          |
| Category               | 33498724          |
| Quantity               | 4000000           |
| UnitPrice              | 4000000           |
| PaymentType            | 33501772          |
| Country                | 33817087          |
| TotalPrice             | 4000000           |
| **Total memory usage** | **178 megabytes** |

---

## Pandas with pyarrow:

### Average time: Average execution time over 100 executions: 0.0355173015499895 seconds

### Memory:

| Field                  | Value            |
|------------------------|------------------|
| Index                  | 128              |
| UserID                 | 4000000          |
| TransactionDate        | 2000000          |
| ProductID              | 4000000          |
| ProductName            | 5600860          |
| Category               | 6998724          |
| Quantity               | 4000000          |
| UnitPrice              | 4000000          |
| PaymentType            | 7001772          |
| Country                | 7317087          |
| TotalPrice             | 4000000          |
| **Total memory usage** | **47 megabytes** |

---

## Spark with inferSchema=True

### Average time: Average execution time over 100 executions: 0.14822416417999193 seconds

### Memory:

| Field                  | Value            |
|------------------------|------------------|
| **Total memory usage** | **40 megabytes** |

---

## Spark with schema

### Average time: Average execution time over 100 executions: 0.032726224859979995 seconds

### Memory:

| Field                  | Value            |
|------------------------|------------------|
| **Total memory usage** | **40 megabytes** |

---

## Dask without pyarrow

### Average execution time over 100 executions: 0.007048575499975413 seconds (without compute())

### Average execution time over 100 executions: 0.0911808580199795 seconds (with compute())

### Memory:

| Field                  | Value            |
|------------------------|------------------|
| Category               | 8998724          |
| Country                | 9317087          |
| Index                  | 128              |
| PaymentType            | 9001772          |
| ProductID              | 4000000          |
| ProductName            | 7600860          |
| Quantity               | 4000000          |
| TotalPrice             | 4000000          |
| TransactionDate        | 9000000          |
| UnitPrice              | 4000000          |
| UserID                 | 4000000          |
| **Total memory usage** | **61 megabytes** |

---

## Dask with pyarrow

### Average execution time over 100 executions: 0.006354925759987964 seconds (without compute())

### Average execution time over 100 executions: 0.0054287596100039085 seconds (with compute())

### Memory:

| Field                  | Value            |
|------------------------|------------------|
| Category               | 6998724          |
| Country                | 7317087          |
| Index                  | 128              |
| PaymentType            | 7001772          |
| ProductID              | 4000000          |
| ProductName            | 5600860          |
| Quantity               | 4000000          |
| TotalPrice             | 4000000          |
| TransactionDate        | 7000000          |
| UnitPrice              | 4000000          |
| UserID                 | 4000000          |
| **Total memory usage** | **51 megabytes** |

---


