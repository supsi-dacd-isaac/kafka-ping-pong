# kafka-ping-pong

***How to play:***

Requirements:
<pre>
Kafka running on socket localhost:9092
Topics ping2pong and pong2ping activated on Kafka server
</pre>

1) Start player 1 (ping)
<pre>
python3 player.py --bootstrap_server localhost:9092 --group scrooge --producer_topic ping2pong --consumer_topic pong2ping --stuff_length 8 --delay 2
</pre>

2) Start player 2 (pong)
<pre>
python3 player.py --bootstrap_server localhost:9092 --group scrooge --producer_topic pong2ping --consumer_topic ping2pong --stuff_length 8 --delay 2
</pre>

3) Start the game
<pre>
python3 starter.py --bootstrap_server localhost:9092 --topic pong2ping
</pre>
