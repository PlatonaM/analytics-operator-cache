## analytics-operator-cache

Cache messages from a batch upload and output collected messages as a single message.

### Configuration

`time_input`: Name of input used for timestamps.

`batch_pos_input`: Name of input used for batch info.

`batch_pos_start`: Keyword for batch start.

`batch_pos_end`: Keyword for batch end.

`time_window`: Relative window in seconds for caching messages.

`compressed_output`: Compress output messages.

`nested_map_inputs`: List of inputs with nested maps.

`logging_level`: Set logging level to `info`, `warning`, `error` or `debug`.

### Inputs

User defined via GUI.

### Outputs

`cached_data`: Collected messages (JSON).
