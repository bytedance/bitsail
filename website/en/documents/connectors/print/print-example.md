# Print connector examples

Parent documents: [print-connector](./print.md)

-----

## Print write connector

the data can be printed out with the following configuration.

```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.legacy.print.sink.PrintSink",
      "batch_size": "10"
    }
  }
}
```