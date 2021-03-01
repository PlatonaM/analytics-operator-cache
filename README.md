#### analytics-operator-cache

Cache messages from a batch upload and output collected messages as a single message.

#### config options

+ `time_input`: name of input used for timestamps.
+ `batch_pos_input`: name of input used for batch info.
+ `batch_pos_start`: keyword for batch start.
+ `batch_pos_end`: keyword for batch end.
+ `time_window`: relative window in seconds for caching messages.
+ `compress_output`: compress output messages `true` / `false`.

