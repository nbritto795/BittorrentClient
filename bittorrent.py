#!/usr/bin/env python3
import sys
import bencoding
import requests
import hashlib
import asyncio
import random
import socket
import traceback
from datetime import datetime
from typing import OrderedDict
import ipaddress
import timeit 

DEBUG = True
DEBUG_SERVER = True

class Torrent:
	def __init__(self, tracker_url, parameters, num_pieces, piece_length, file_size, our_id, output_file):
		self.tracker_url = tracker_url
		self.parameters = parameters
		self.num_pieces = num_pieces
		self.piece_length = piece_length
		self.file_size = file_size
		self.our_id = str.encode(our_id)
		self.output_file = output_file
		self.connected_peers = {}
		self.external_ip = ''
		self.internal_ip = ''
		self.port = parameters['port']
		self.finished_downloading = False
		self.finished_num = 0
		self.compact = parameters['compact']
		self.seeding_peers = []

		# will store which peer is in charge of downloading which piece
		# None --> Peer.id that is in charge --> Finished state
		self.pieces_to_peer = {}
		for i in range(int(self.num_pieces)):
			self.pieces_to_peer[i] = None
		self.piece_internal_index = []
		for i in range(int(self.num_pieces)):
			self.piece_internal_index.append(0)

	def get_tracker_url(self):
		return self.tracker_url

	def get_parameters(self):
		return self.parameters

	def get_num_pieces(self):
		return self.num_pieces

	def get_piece_length(self):
		return self.piece_length

	def get_file_size(self):
		return self.file_size

	def get_our_id(self):
		return self.our_id

	def set_external_ip(self, external_ip):
		self.external_ip = external_ip

	def set_internal_ip(self, internal_ip):
		self.internal_ip = internal_ip

	def get_external_ip(self):
		return self.external_ip

	def get_internal_ip(self):
		return self.internal_ip

	def get_port(self):
		return self.port

	def get_pieces(self):
		return self.pieces_to_peer

	def get_compact(self):
		return self.compact

	async def finished(self):
		return self.finished_downloading

	def get_peers_we_are_seeding(self):
		return self.seeding_peers

	# requests peers
	async def request_peers(self):
		# a workaround to avoid aiohttp
		# response = requests.get(torrent.get_tracker_url(), torrent.get_parameters())
		# Send a get request to the tracker to get a list of peers, the response will be a bencoded response with the peers and what they have
		response = await loop.run_in_executor(None, requests.get, self.get_tracker_url(), self.get_parameters())
		# if the response is compact, we need to break out the bytes of information, parse through and place them in an info dict so we dont have
		# to change the rest of our code. Note: sometimes even if compact is set to 0, we will still get a compact response, so it's probably best
		# if we keep compact set to 1
		if self.parameters['compact'] == 1:
			response_parsed = response.content
			if DEBUG:
				print("\nRESPONSE PARSED:\n", response_parsed)
			response_parsed = bencoding.bdecode(response_parsed)
			list_of_peers_binary = list(response_parsed.items())[3][1]

			if DEBUG:
				print("\nLIST OF PEERS: ", list_of_peers_binary)
			offset = 0
			length = int(len(list_of_peers_binary)/6)
			x = 0
			list_of_dictionaries = []
			# the first 4 bytes of every 6 bytes represents the ip address and the last 2 represent the port, so break each out and store in an info dict
			for x in range(length):
				ip = list_of_peers_binary[offset:offset+4]
				ip_converted = str(ipaddress.ip_address(ip))
				dict = OrderedDict()
				print("ip addr: ", ip_converted)
				offset += 4
				port = list_of_peers_binary[offset:offset+2]
				port_int = int.from_bytes(port, "big")
				print("port: ", port_int)
				offset += 2
				dict['ip'.encode('utf-8')] = ip_converted.encode('utf-8')
				dict['peer id'.encode('utf-8')] = 'peer_id'.encode('utf-8')
				dict['port'.encode('utf-8')] = port_int
				list_of_dictionaries.append(dict)
			return list_of_dictionaries
		   # otherwise breakout list of peers and return it after decoding
		else:
			# response_parsed = bytes(response.text, 'utf-8')
			response_parsed = response.content
			if DEBUG:
				print("\nRESPONSE PARSED:\n", response_parsed)
			response_parsed = bencoding.bdecode(response_parsed)
			list_of_peers = response_parsed[b'peers']
			if DEBUG:
				print("\nLIST OF PEERS: ", list_of_peers)
			return list_of_peers

	# handles incoming connections requesting data

	async def handle_requests(self, reader, writer):

		connection_info = reader._transport.get_extra_info('peername')
		if DEBUG_SERVER:
			print(f"{reader._transport.get_extra_info('peername')}")
			print(f"{writer._transport.get_extra_info('peername')}")
			print(f"{reader._transport.get_extra_info('sockname')}")
			print(f"{writer._transport.get_extra_info('sockname')}")

		if connection_info not in self.connected_peers.keys():
			new_peer = Peer(None, self.num_pieces,
							self.piece_length, self.file_size)
			new_peer.set_ip(connection_info[0])
			new_peer.set_port(connection_info[1])
			new_peer.set_reader_writer(reader, writer)
			self.seeding_peers.append(new_peer)
			await new_peer.seeder(self)


	# gets the current block that is not being requested
	async def which_piece_to_request(self, peer_id, bitfield):
		pieces_that_peer_has = set([i for i, x in enumerate(bitfield) if x])
		pieces_needed = [i for i, x in enumerate(
			self.pieces_to_peer.items()) if x[1] is None]

		if DEBUG:
			print(f"Pieces needed: {pieces_needed}")
			print(f"Pieces peer has: {pieces_that_peer_has}")

		for index in pieces_needed:
			if index in pieces_that_peer_has:
				self.pieces_to_peer[index] = peer_id
				if index != self.get_num_pieces() - 1:
					total_bytes_of_piece = self.get_piece_length()
					if (self.piece_internal_index[index] + 16384 <= total_bytes_of_piece):
						self.piece_internal_index[index] += 16384
						print(f"Get piece index: {index} and subindex: {(self.piece_internal_index[index] - 16384)} from {peer_id}")

						# If this was not the complete piece reset the pieces_to_peer[index] to be None in write_to_file
						return index, (self.piece_internal_index[index] - 16384), 16384
					else:
						num_bytes = (total_bytes_of_piece - (self.assigned_pieces_index[index]))
						self.assigned_pieces_index[index] += num_bytes

						# If this was not the complete piece reset the pieces_to_peer[index] to be None in write_to_file
						return index, (self.piece_internal_index[index] - num_bytes), num_bytes
				else:
					print("A last piece request")
					total_bytes_of_piece = self.get_file_size() % self.get_piece_length()
					if (self.piece_internal_index[index] + 16384 <= total_bytes_of_piece):
						self.piece_internal_index[index] += 16384
						print(f"Get piece index: {index} and subindex: {(self.piece_internal_index[index] - 16384)} from {peer_id}")

						# If this was not the complete piece reset the pieces_to_peer[index] to be None in write_to_file
						return index, (self.piece_internal_index[index] - 16384), 16384
					else:
						num_bytes = (total_bytes_of_piece - (self.piece_internal_index[index]))
						self.piece_internal_index[index] += num_bytes

						# If this was not the complete piece reset the pieces_to_peer[index] to be None in write_to_file
						return index, (self.piece_internal_index[index] - num_bytes), num_bytes
		return None, None, None
	
	async def send_have_to_peers(self, index):
		for peer in self.get_peers_we_are_seeding():
			await peer.send_have(index)

	async def write_to_file(self, piece_index, peer_id, piece_length, start_of_block, data):
		assert self.pieces_to_peer[piece_index] == peer_id, 'peer id wrong wrong file writing'
		print(f"Got index {piece_index} from {peer_id}")
		if DEBUG:
			print(
				f"writing data to {piece_index * piece_length + start_of_block}")
		self.output_file.seek(piece_index * piece_length + start_of_block)
		self.output_file.write(data)

		if len(data) == piece_length or start_of_block + len(data) == piece_length or (start_of_block + len(data) == self.get_file_size() % piece_length and piece_index == self.get_num_pieces() - 1):
			self.finished_num += 1
			self.pieces_to_peer[piece_index] = (f'FINISHED:{peer_id}')
			print(self.finished_num)
			await self.send_have_to_peers(piece_index)
		else:
			self.pieces_to_peer[piece_index] = None

		

		if self.finished_num == self.num_pieces:
			self.finished_downloading = True

	async def get_bytes(self, index, begin, length_to_send):
		self.output_file.seek(index * self.piece_length + begin)
		bytes_to_send = self.output_file.read(length_to_send)
		return bytes_to_send


class Peer:
	def __init__(self, peer, num_pieces, piece_length, file_size):
		if peer is not None:
			self.ip = peer[b'ip'].decode('utf-8')
			self.id = peer[b'peer id']
			self.port = peer[b'port']
		else:
			self.ip = None
			self.id = None
			self.port = None
		self.peer_choking = True
		self.peer_interested = False
		self.pieces = []
		self.piece_length = piece_length
		self.file_size = file_size

		# Available pieces initially set to false
		for i in range(int(num_pieces)):
			self.pieces.append(False)

	def set_id(self, id):
		self.id = id

	def set_ip(self, ip):
		self.ip = ip

	def set_port(self, port):
		self.port = port

	def set_reader_writer(self, reader, writer):
		self.reader = reader
		self.writer = writer

	def info(self):
		print("ip: ", self.ip)
		print("id: ", self.id)
		print("port: ", self.port)

	async def send_handshake(self, torrent):
		# construct handshake
		our_id = torrent.get_our_id()
		handshake = b''.join([chr(19).encode(), b'BitTorrent protocol', (chr(
			0) * 8).encode(), torrent.parameters['info_hash'], our_id])

		# write handshake to peer
		self.writer.write(handshake)
		await self.writer.drain()

		if DEBUG:
			print(f'\n\nSend handshake to: {self.ip}:{self.port} {self.id}')

	async def validate_handshake(self, torrent):

		# Read response
		peer_handshake = await self.reader.read(68)
		if DEBUG:
			print(f'\n\nRecieved handshake {self.ip}:{self.port}')

		# validate handshake
		peer_id_check = peer_handshake[48:]
		info_hash_check = peer_handshake[28:48]

		if info_hash_check == torrent.parameters['info_hash']:
			# if self.id is None or peer_id_check == self.id:
				if DEBUG:
					print(
						f"\n\n--Validate Handshake Complete for {self.ip}:{self.port} {self.id}--")
				return True
		if DEBUG:
			print(
				f"\n\n--Validate Handshake FAILED for {self.ip}:{self.port} {self.id}--")
			print(info_hash_check, ' should equal ',
				  torrent.parameters['info_hash'])
			print(peer_id_check, ' should equal ', self.id)
		return False

	async def send_unchoke(self):
		length = 1
		unchoke_msg = b''.join(
			[length.to_bytes(4, byteorder='big'), chr(1).encode()])
		if DEBUG_SERVER:
			print(f"\n\nSending Unchoke: to {self.id} {unchoke_msg}")
		self.writer.write(unchoke_msg)
		await self.writer.drain()
		
	async def send_have(self, index):
		length = 5
		have_msg = b''.join(
			[length.to_bytes(4, byteorder='big'), chr(4).encode(), index.to_bytes(4, 'big')])
		if DEBUG_SERVER:
			print(f"\n\nSending Have {index}: to {self.id} {have_msg}")
		self.writer.write(have_msg)
		await self.writer.drain()


	async def send_bitfield(self, torrent):
		# construct bitfield message
		pieces = torrent.get_pieces()
		# find closest greater multiple of 8
		length = ((len(pieces.keys()) + 7) & (-8))

		binary_string = ''

		# construct binary string
		for (piece_index, peer_id) in pieces.items():
			if peer_id is not None and not isinstance(peer_id, (bytes, bytearray)) and 'FINISHED' in peer_id:
				binary_string += '1'
			else:
				binary_string += '0'

		# pad with 0s
		for i in range(length - len(binary_string)):
			binary_string += '0'

		# convert to hex
		hex_string = hex(int(binary_string, 2))

		# divide length to be bytes instead of bits
		length = (int)(length / 8) + 1

		if DEBUG_SERVER:
			print(f"Length of bitfield will be: {length}")
			print(f"Binary string: {binary_string}")
			print(f"Hex string: {hex_string}")

		# construct message
		bitfield_msg = b''.join([length.to_bytes(4, byteorder='big'), chr(
			5).encode(), bytearray.fromhex(hex_string[2:])])

		# write handshake to peer
		self.writer.write(bitfield_msg)
		await self.writer.drain()

		if DEBUG_SERVER:
			print(f"Sending hex string: {bitfield_msg}")

	async def send_piece(self, index, begin, bytes_to_send):
		# construct message
		length = len(bytes_to_send) + 9
		piece_msg = b''.join([length.to_bytes(4, byteorder='big'), chr(7).encode(), index.to_bytes(
			4, byteorder='big'), begin.to_bytes(4, byteorder='big'), bytes_to_send])
		self.writer.write(piece_msg)
		await self.writer.drain()

	async def request_piece(self, torrent):
		# if we are not interested, send an interested message
		if not self.peer_interested:
			length = 1
			interested_msg = b''.join(
				[length.to_bytes(4, byteorder='big'), chr(2).encode()])
			if DEBUG:
				print(f"\n\nSending Interested: to {self.id} {interested_msg}")
			self.writer.write(interested_msg)
			await self.writer.drain()
			self.peer_interested = True
			return

		# if we are interested and not choking send a request
		if not self.peer_choking:
			msg_length = 13
			# get index from torrent HERE
			piece_index, start_index, num_bytes = await torrent.which_piece_to_request(self.id, self.pieces)
			if piece_index is None:
				return
			else:
				if DEBUG:
					print(
						f"Requesting piece: {piece_index} from {self.ip}:{self.port}")
				request_msg = b''.join([msg_length.to_bytes(4, byteorder='big'), chr(6).encode(), piece_index.to_bytes(
					4, byteorder='big'), start_index.to_bytes(4, byteorder='big'), num_bytes.to_bytes(4, byteorder='big')])

			self.writer.write(request_msg)
			await self.writer.drain()
			return

		# if we are choked, then dont do anything wait for unchoke

	async def set_available_pieces(self, bitfield):
		if DEBUG:
			print(f"bitfield:{bitfield}")
		one_bit_to_shift = 128
		one_bit_to_shift = one_bit_to_shift.to_bytes(
			len(bitfield), byteorder='little')
		if DEBUG:
			print(f"One bit to shift: {one_bit_to_shift}")
		for i in range(len(self.pieces)):
			if (int.from_bytes(one_bit_to_shift, 'big') >> i) & int.from_bytes(bitfield, 'big') != 0:
				self.pieces[i] = True
		if DEBUG:
			print(f"Pieces: {self.pieces}")

	async def digest_message(self):
		msg_length = await self.reader.read(4)
		if len(msg_length) < 4:
			return
		msg_length = int.from_bytes(msg_length, "big")

		if DEBUG:
			print(
				f'\nmsg length: {msg_length} from {self.ip}:{self.port}, {self.id}')

		msg_id = None
		msg_payload = None
		if msg_length > 0:
			msg_id = await self.reader.read(1)
			msg_id = int.from_bytes(msg_id, "big")
			if msg_length > 1:
				msg_payload = await self.reader.read(msg_length - 1)
				while len(msg_payload) != (msg_length - 1):
					if DEBUG:
						print("need to read more")
					msg_payload += await self.reader.read(msg_length - 1 - len(msg_payload))
				if DEBUG:
					print(f"Length of msg payload: {len(msg_payload)}")
			if DEBUG:
				print(f"MSG ID: {msg_id}")

		return msg_length, msg_id, msg_payload

	async def seeder(self, torrent):
		handshake_passed = await self.validate_handshake(torrent)

		if handshake_passed or torrent.get_compact() == 1:
			await self.send_handshake(torrent)

			# send a bitfield message
			await self.send_bitfield(torrent)

			while True:
				# await asyncio.sleep(0.02)
				try:
					msg_length, msg_id, msg_payload = await self.digest_message()
				except TypeError:
					print("FINISHED")
					break

				if msg_length > 0:
					print(f"\n\n(Server)Recieved data:\n{msg_payload}")
					# they send us interested
					if msg_id == 2:
						print("(Server)Interested Message")
						self.peer_interested = True

						# send unchoke
						await self.send_unchoke()
						self.peer_choking = False

					# they send us un interested
					elif msg_id == 3:
						self.peer_interested = False
						print("(Server)Uninterested Message")
						# send choke?

					elif msg_id == 6:
						print("(Server)Request Message")
						index = int.from_bytes(msg_payload[0:4], "big")
						begin = int.from_bytes(msg_payload[4:8], "big")
						length_to_send = int.from_bytes(
							msg_payload[8:12], "big")

						# respond with bytes
						bytes_to_send = await torrent.get_bytes(index, begin, length_to_send)
						await self.send_piece(index, begin, bytes_to_send)
					else:
						print(f"Server got {msg_id}")
				else:
					# Keep alive
					print("(Server)Keep Alive Message")

	async def download(self, torrent):

		# set up connection
		if DEBUG:
			print(f'\n\nTrying to connect to {self.ip}:{self.port}')

		# handle connections with local network self
		local_machine_ip = torrent.get_external_ip()
		internal_ip = torrent.get_internal_ip()
		if self.ip == local_machine_ip:
			if self.port == torrent.get_port():
				if DEBUG:
					print("Trying to connect to self port. Aborting.")

				return
			else:
				if DEBUG:
					print(
						f'Trying to connect to self, converting ip {self.ip} to {internal_ip}')

				self.ip = internal_ip

		connect = asyncio.open_connection(self.ip, self.port)
		try:
			# Wait for 1 second before abandoning
			reader, writer = await asyncio.wait_for(connect, timeout=1)
			self.reader = reader
			self.writer = writer
		except asyncio.TimeoutError:
			if DEBUG:
				print(f'\n\nTimeout, skipping {self.ip}:{self.port}')
				return
		except:
			if DEBUG:
				print(f'\n\nOther connection error for {self.ip}:{self.port}')
				print(traceback.format_exc())
				return

		# now execute handshake and check if correct
		await self.send_handshake(torrent)
		succesful_handshake = await self.validate_handshake(torrent)

		# should we send interested immediately?

		if succesful_handshake or torrent.get_compact() == 1:
			# Begin exchanging data
			while True:
				fin = await torrent.finished()
				if DEBUG:
					print(f"Finished", fin)
				if fin:
					break
				# await asyncio.sleep(0.02)
				msg_length, msg_id, msg_payload = await self.digest_message()
				if msg_length > 0:
					if msg_id == 0:
						print("Choke Message")
						self.peer_choking = True

					elif msg_id == 1:
						print("Unchoke Message")
						self.peer_choking = False
						print(len(self.pieces))

					# they send us interested -- handle in server
					elif msg_id == 2:
						print("Interested Message")

					# they send us un interested -- handle in server
					elif msg_id == 3:
						print("Uninterested Message")

					elif msg_id == 4:
						print("Have Message")
						self.pieces[int.from_bytes(msg_payload, 'big')] = True

					elif msg_id == 5:
						print("Bitfield Message")
						print(msg_payload)
						await self.set_available_pieces(msg_payload)

					# they send us request -- handle in server
					elif msg_id == 6:
						print("Request Message")

					elif msg_id == 7:
						# if piece recieved is not full block then request again with offset
						print("Piece Message")
						piece_index = int.from_bytes(msg_payload[0:4], 'big')
						start_of_block = int.from_bytes(
							msg_payload[4:8], 'big')
						data = msg_payload[8:]

						await torrent.write_to_file(piece_index, self.id, self.piece_length, start_of_block, data)

					else:
						print("unrecognized id of: ", msg_id)
				else:
					# Keep alive
					print("Keep Alive Message")

				await self.request_piece(torrent)

		# after finished downloading close connection
		fin = await torrent.finished()
		end_time = timeit.default_timer()

		if DEBUG and fin:
			for index, peer in torrent.get_pieces().items():
				print(f"index: {index} ---> peer:{peer}")
		
		print(f"ID: {torrent.get_our_id()}")
		print(f"Time to download = {end_time - torrent.time_start}")
		print(f"IP Internal: {torrent.get_internal_ip()}")
		print(f"IP External: {torrent.get_external_ip()}")
		print(f"PORT: {torrent.get_port()}")
		self.writer.close()
		await self.writer.wait_closed()

		print("CLOSED")


def parse_torrent(file_name):
	with open(file_name, 'rb') as file:
		bytes_data = file.read()
		torrent = bencoding.bdecode(bytes_data)
	now_t = datetime.now().strftime("%H-%M-%S-%f-")
	name_of_file = now_t + torrent[b'info'][b'name'].decode('utf-8')
	print(name_of_file)
	output_file = open(name_of_file, 'w+b')

	# isolate URL of tracker
	tracker_url = torrent[b'announce']

	# isolate info dict
	info_dict = torrent[b'info']

	# sha 1 hash using digest instead of hexdigest
	info_hash = hashlib.sha1(bencoding.bencode(info_dict)).digest()

	# generate peer id
	peer_id = '-PD0001-' + \
		''.join([str(random.randint(0, 9)) for x in range(12)])

	parameters = {
		'info_hash': info_hash,
		'peer_id': peer_id,
		'port': PORT,
		'event': 'started',
		'uploaded': 1,
		'downloaded': 0,
		'left': info_dict[b'length'],
		'compact': 0
	}
	return Torrent(tracker_url, parameters, len(info_dict[b'pieces'])/20, info_dict[b'piece length'], info_dict[b'length'], peer_id, output_file)


async def set_up_server(torrent):
	hostname = socket.gethostname()
	local_ip = socket.gethostbyname(hostname)

	external_ip = requests.get('https://api.ipify.org').text
	if DEBUG_SERVER:
		print(f"\n\nExternal ip {external_ip}")

	# local_ip = '0.0.0.0'

	torrent.set_external_ip(external_ip)
	torrent.set_internal_ip(local_ip)

	if DEBUG_SERVER:
		print(f"\n\nLOCAL IP IS {local_ip}:{PORT}/{torrent.our_id}")
	server = await asyncio.start_server(torrent.handle_requests, local_ip, PORT)
	if DEBUG_SERVER:
		print("Awaited server start")
	async with server:
		await server.serve_forever()


async def start_download_and_upload(file_name):
	now = datetime.now().strftime("-%H-%M-%S-%f")
	# name file a unique name based on time -- make sure multiple aren't started same second though

	torrent = parse_torrent(file_name)
	if DEBUG:
		print("\n\n:Parameters:")
		print(torrent.tracker_url)
		print(torrent.parameters)
	peers_data = await torrent.request_peers()
	if DEBUG:
		print("\n\nPeer's Data:")
		print(peers_data)
	peers = []
	for peer in peers_data:
		peers.append(Peer(peer, torrent.get_num_pieces(),
					 torrent.get_piece_length(), torrent.get_file_size()))
	if DEBUG:
		for p in peers:
			print("\nINFO:")
			p.info()

	# start server task
	tasks = [set_up_server(torrent)]

	# add connect to peer tasks
	for peer in peers:
		# if peer.ip == '128.8.130.84':
		# if peer.ip == '108.28.227.189':
		tasks.append(peer.download(torrent))
	# start all tasks
	time_start = timeit.default_timer()
	torrent.time_start = time_start
	res = await asyncio.gather(*tasks, return_exceptions=True)
	return res

PORT = random.randint(50000, 60000)
if __name__ == "__main__":
	torrent_file = sys.argv[1]
	loop = asyncio.get_event_loop()
	loop.run_until_complete(start_download_and_upload(torrent_file))
	loop.close()
