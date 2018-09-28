# converter

The heart of the Triton Media platform


## What does this do?

Converter works with `events` to convert all media to a standardized format using a Redis job queue system: Kue. It is a wrapper
around `ffprobe` and `Handbrake`.

## Advantages to "orchestrating" converting

When used with [tritonmedia/autoscaler](https://github.com/tritonmedia/autoscaler) you're able to scale based on jobs in a queue, and utilize
6 core boxes (say on GKE, and preemptible) to save a lot of money while converting media quickly.

## License

MIT