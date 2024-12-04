import sys, traceback, threading, socket

from VideoStream import VideoStream
from RtpPacket import RtpPacket

class ServerWorker:	
    
    
###############################################
#	Enviar pacotes RTP de vídeo para pedinte
###############################################

	def sendRtp(self):
		"""Send RTP packets over UDP."""
		while True:
			self.clientInfo['event'].wait(0.05)
			
			# Stop sending if request is PAUSE or TEARDOWN
			if self.clientInfo['event'].isSet():
				break
				
			data = self.clientInfo['videoStream'].nextFrame()
			if data:
				frameNumber = self.clientInfo['videoStream'].frameNbr()
				try:
					address = self.clientInfo['rtpAddr']
					port = int(self.clientInfo['rtpPort'])
					packet =  ServerWorker.makeRtp(data, frameNumber)
					msg = {destination: 1 , 'payload': packet}

					self.clientInfo['rtpSocket'].sendto(packet,(address,port))
				except:
					print("Connection Error")
					print('-'*60)
					traceback.print_exc(file=sys.stdout)
					print('-'*60)
		# Close the RTP socket
		self.clientInfo['rtpSocket'].close()
		# print("All done!")

#########################################################################
#		Construção de um pacote RTP contendo os dados do vídeo
#########################################################################

	@staticmethod
	def makeRtp(payload, frameNbr):
		"""RTP-packetize the video data."""
		version = 2
		padding = 0
		extension = 0
		cc = 0
		marker = 0
		pt = 26 # MJPEG type
		seqnum = frameNbr
		ssrc = 0
		
		rtpPacket = RtpPacket()
		
		rtpPacket.encode(version, padding, extension, cc, seqnum, marker, pt, ssrc, payload)
		# print("Encoding RTP Packet: " + str(seqnum))
		
		return rtpPacket.getPacket()

	def stop_serving(self):
		self.clientInfo['event'].set()


#################################
#		Inciar stream
#################################

	def serve_movie(self, dest_addr:str, dest_port:int, movie_name:str, videos_dir:str="./videos/"):
		filename = f"{videos_dir}{movie_name}" 

		self.clientInfo = dict()

		# videoStream
		self.clientInfo['videoStream'] = VideoStream(filename)
		# socket de quem pede o vídeo
		# rtpPort -> A porta UDP do pedinte
		# rtpAddr -> ip do pedinte
		self.clientInfo['rtpPort'] = dest_port
		self.clientInfo['rtpAddr'] = dest_addr
		print("Mandando vídeo para:" + self.clientInfo['rtpAddr'] + ":" + str(self.clientInfo['rtpPort']))
		# Create a new thread and start sending RTP packets
		self.clientInfo["rtpSocket"] = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.clientInfo['event'] = threading.Event()
		self.clientInfo['worker']= threading.Thread(target=self.sendRtp)
		self.clientInfo['worker'].start()
