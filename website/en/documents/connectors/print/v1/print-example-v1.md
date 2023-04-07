# Print connector V1 examples

Parent documents: [print-connector](./print-v1.md)

-----

## Print write connector

the data can be printed out with the following configuration.


```json
{
  "job": {
    "writer": {
      "class": "com.bytedance.bitsail.connector.print.sink.PrintSink",
      "sample_write": true,
      "sample_limit": 10
    }
  }
}
```