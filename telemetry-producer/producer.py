import argparse
import json
from confluent_kafka import Producer

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input",type=str, help="Input file", required=True)
    parser.add_argument("-t", "--topic", type=str, help="topic", default="telemetry.out")
    parser.add_argument("-b", "--bootstrap-server", type=str, help="bootstrap server", default="localhost:9092")
    args = parser.parse_args()

    p = Producer({ "bootstrap.servers": args.bootstrap_server })
    with open(args.input) as f:
        for line in f:
            try:
                json.loads(line)
                p.produce(topic=args.topic, value=line)
                p.flush()
            except ValueError:
                pass
