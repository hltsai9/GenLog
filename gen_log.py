#!/usr/bin/python
import random
import datetime
import time
import sys
import argparse
import threading
'''
 cmdline = argparse.ArgumentParser(usage="usage: genLog.py --line",
								  description="")
'''
class FlumeComponent(object):
	def __init__(self, template_type, gen_volume):
		self.event_cnt = 0
		self.byte_cnt = 0
		self.template_type = template_type
		self.gen_volume = gen_volume
#		self.template = ""

	def increase_event_cnt(self, increment):
		self.event_cnt += increment

	def increase_byte_cnt(self, increment):
		self.byte_cnt += increment

	def print_event(self, template, ec, bc):
		current_time = ','.join(str(datetime.datetime.now()).split('.'))[:-3]
		print("%s %s [%d] events with [%d] bytes [0] fails." % (current_time,template,ec,bc))


class Source(FlumeComponent):
	def __init__(self, template_type, gen_volume, file_channel):
		super(Source, self).__init__(template_type, gen_volume)
		self.template = "INFO  (%sSource.xxx:942) - [flume_%ssrc] " % (self.template_type,self.template_type.lower())
		self.file_channel = file_channel

	def receive_event(self, received_count):
		self.event_cnt = received_count
		self.byte_cnt = received_count*1024
		self.file_channel.put_event(received_count)

	def print_event_periodically(self, total_time, update_time):
		record_time = 0
		while (record_time <= total_time):
			self.print_event(self.template, self.event_cnt, self.byte_cnt)
			time.sleep(update_time)
			record_time += update_time

class Sink(FlumeComponent):
	def __init__(self, template_type, gen_volume, file_channel):
		super(Sink, self).__init__(template_type, gen_volume)
		self.template = "INFO  (%sSink.xxx:942) - [flume_%ssink]" % (self.template_type,self.template_type.lower())
		self.file_channel = file_channel

	def drain_event(self, total_time, drain_time, drained_count):
		record_time = 0

		while (record_time <= total_time) or (self.file_channel.event_cnt != 0):
			real_drain = 0
			if drained_count > self.file_channel.event_cnt:
				real_drain = self.file_channel.event_cnt
			else:
				real_drain = drained_count
			if real_drain > 0:
				self.file_channel.consume_event(real_drain)
				self.print_put(real_drain)
			self.event_cnt = real_drain
			self.byte_cnt = real_drain*1024
			time.sleep(drain_time)
			record_time += drain_time

	def print_event_periodically(self, total_time, update_time):
		record_time = 0
		while (record_time <= total_time) or (self.file_channel.event_cnt != 0):
			self.print_event(self.template, self.event_cnt, self.byte_cnt)
			time.sleep(update_time)
			record_time += update_time

	def print_put(self, event_consumed):
		current_time = ','.join(str(datetime.datetime.now()).split('.'))[:-3]
		if self.gen_volume:
			for _ in range(event_consumed):			
				print("%s INFO  (TsmcHBase.xxx:333) - Put RowKey" % (current_time))
		else:
			print("%s INFO  (TsmcHBase.xxx:333) - Put RowKey * %d" % (current_time, event_consumed))

class Channel(FlumeComponent):
	def __init__(self, template_type, gen_volume):
		super(Channel, self).__init__(template_type, gen_volume)
		self.template = "INFO  (%sFC.xxx:942) - [flume_%sfc]" % (self.template_type,self.template_type.lower())

	def put_event(self, received_count):
		self.increase_event_cnt(received_count)
		self.increase_byte_cnt(received_count*1024)

	def consume_event(self, consumed_count):
		self.increase_event_cnt(-consumed_count)
		self.increase_byte_cnt(-consumed_count*1024)

	def print_event_periodically(self, total_time, update_time):
		record_time = 0
		while (record_time <= total_time) or (self.event_cnt != 0):
			self.print_event(self.template, self.event_cnt, self.byte_cnt)
			time.sleep(update_time)
			record_time += update_time


class FlumeAgent(object):
	def __init__(self, template_type, gen_volume):
		self.template_type = template_type
		self.gen_volume = gen_volume
		self.channel = Channel(template_type, gen_volume)
		self.source = Source(template_type, gen_volume, self.channel)
		self.sink = Sink(template_type, gen_volume, self.channel)

	def start(self, total_time, update_time):
		self.source_status = threading.Thread(target=self.source.print_event_periodically, args=(total_time, update_time))
		self.channel_status = threading.Thread(target=self.channel.print_event_periodically, args=(total_time, update_time))
		self.sink_status = threading.Thread(target=self.sink.print_event_periodically, args=(total_time, update_time))


class Connection(object):
	def __init__(self, min_event, max_event, flume_agent):
		self.min_event = min_event
		self.max_event = max_event
		self.flume_agent = flume_agent
		self.ip = "10.10.10.%d" % (random.randint(1,200))
		self.gen_open()

	def gen_open(self):
		current_time = ','.join(str(datetime.datetime.now()).split('.'))[:-3]
		print("%s INFO  (NNNNNNNNsystem:111) - %s connection OPEN" % (current_time,self.ip))

	def gen_data(self):
		event_increased = random.randint(self.min_event, self.max_event)
		self.flume_agent.source.receive_event(event_increased)


if __name__ == "__main__":

	# General config
	gen_volume = False
	total_time = 30.0
	# Log updates every update_time seconds
	update_time = 1.0
	# Source drains event every drain_time seconds, reduce events with drain_rate
	drain_time = 0.5
	drain_rate = 400
	# Connection count every per_time seconds, each connection will generate min_event~max_event events 
	# Total connection count will be connection_cnt*(total_time/per_time)
	connection_cnt = 5
	per_time = 1.0
	min_event = 100
	max_event = 300


	agent = FlumeAgent("HBase", gen_volume)
	agent.start(total_time, update_time)
	agent.source_status.start()
	time.sleep(0.1)
	agent.channel_status.start()
	time.sleep(0.1)
	agent.sink_status.start()
	

	sink_drain = threading.Thread(target=agent.sink.drain_event, args=(total_time, drain_time, drain_rate))
	sink_drain.start()

	
	for i in range(int(total_time/per_time)):
#		print("batch: %d/%d" % (i,int(total_time/per_time)))
		connection_list=[]
		for _ in range(connection_cnt):
			new_con = Connection(min_event, max_event, agent)
			connection_list.append(new_con)
		for con in connection_list:
			con.gen_data()
			time.sleep(per_time/connection_cnt)	

	sink_drain.join()
	agent.source_status.join()
	agent.channel_status.join()
	agent.sink_status.join()


'''
Source
AppendReceivedCount
The total number of events that came in with only one event per batch (the equivalent of an append call in RPC calls).

Channel
ChannelSize
The total number of events currently in the channel.
ChannelCapacity
The capacity of the channel.
ChannelFillPercentage
The percentage of the channel that is full. Type For channels, this always returns CHANNEL.


Sink
EventDrainSuccessCount
The total number of events that the sink successfully wrote out to storage.
'''
'''
	total_time = 1.0
	per_time = 1.0
	connection_cnt = 55800 # 1GB
	connection_cnt = 27250 # 500MB
	connection_cnt = 5450 # 100MB

	consume_update = 0.2
	consume_rate = 1500000000
	new_con = Connection(300, 300)
'''
