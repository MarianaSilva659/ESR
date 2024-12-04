from tkinter import *
import tkinter.messagebox as tkMessageBox
from PIL import Image, ImageTk
import socket, threading, sys, traceback, os
import random

from RtpPacket import RtpPacket


CACHE_FILE_NAME = f"cache-{random.randint(0,10000)}-"
CACHE_FILE_EXT = ".jpg"

class ClienteStream:
	
	# Initiation..
	def __init__(self, master: Tk, socket):
		self.master = master
		self.master.protocol("WM_DELETE_WINDOW", self.handler)
		self.createWidgets()
		self.rtspSeq = 0
		self.sessionId = 0
		self.requestSent = -1
		self.teardownAcked = 0
		self.rtpSocket = socket
		self.playMovie()
		self.frameNbr = 0
		
#########################################################################
# 		Criação dos botões que são disponibilizados ao cliente
#########################################################################

	def createWidgets(self):
		"""Build GUI."""		
		# Create Play button		
		self.start = Button(self.master, width=20, padx=3, pady=3)
		self.start["text"] = "Play"
		self.start["command"] = self.playMovie
		self.start.grid(row=1, column=1, padx=2, pady=2)
		
		# Create Pause button			
		self.pause = Button(self.master, width=20, padx=3, pady=3)
		self.pause["text"] = "Pause"
		self.pause["command"] = self.pauseMovie
		self.pause.grid(row=1, column=2, padx=2, pady=2)
		
		# Create Teardown button
		self.teardown = Button(self.master, width=20, padx=3, pady=3)
		self.teardown["text"] = "Teardown"
		self.teardown["command"] =  self.handler
		self.teardown.grid(row=1, column=3, padx=2, pady=2)
		
		# Create a label to display the movie
		self.label = Label(self.master, height=19)
		self.label.grid(row=0, column=0, columnspan=4, sticky=W+E+N+S, padx=5, pady=5) 

	
	def exitClient(self):
		"""Teardown button handler."""
		self.master.destroy() 
		os.remove(CACHE_FILE_NAME + str(self.sessionId) + CACHE_FILE_EXT) 

	def pauseMovie(self):
		"""Pause button handler."""
		print('Pause')
		self.playEvent.set()
		# print("Not implemented...")
	
	def playMovie(self):
		"""Play button handler."""
		# Create a new thread to listen for RTP packets
		self.playEvent = threading.Event()
		threading.Thread(target=self.listenRtp).start()
		self.playEvent.clear()
	
 
#########################################################################################
# 		Receber pacotes RTP do ponto de acesso e processá-los para exibir o vídeo
#########################################################################################

	def listenRtp(self):		
		"""Listen for RTP packets."""
		while True:
			try:
				assert not self.playEvent.isSet() # vai para o except caso o user clique em pause ou teardown
				data = self.rtpSocket.recv(65535)
				if data:
					rtpPacket = RtpPacket()
					rtpPacket.decode(data)
					
					currFrameNbr = rtpPacket.seqNum()
					print("Current Seq Num: " + str(currFrameNbr))
										
					if currFrameNbr > self.frameNbr or currFrameNbr < self.frameNbr - 100: # Discard the late packet
						self.frameNbr = currFrameNbr
						self.updateMovie(self.writeFrame(rtpPacket.getPayload()))
				else:
					self.clearDisplay()
					break
			except socket.timeout:
				self.clearDisplay()
				break
			except:
				# Stop listening upon requesting PAUSE or TEARDOWN
				if self.playEvent.isSet(): 
					break
				
				self.rtpSocket.shutdown(socket.SHUT_RDWR)
				self.rtpSocket.close()
				break
				
	def clearDisplay(self):
		self.playEvent.set()
		self.master.destroy() 
		os.remove(CACHE_FILE_NAME + str(self.sessionId) + CACHE_FILE_EXT) 


###############################################################################
# 			Mostra vídeo recebido em um arquivo de imagem
###############################################################################
	def writeFrame(self, data):
		"""Write the received frame to a temp image file. Return the image file."""
		cachename = CACHE_FILE_NAME + str(self.sessionId) + CACHE_FILE_EXT
		file = open(cachename, "wb")
		file.write(data)
		file.close()
		
		return cachename

#########################################################
# 	Atualiza a interface gráfica (GUI) do cliente 
#########################################################
	def updateMovie(self, imageFile):
		"""Update the image file as video frame in the GUI."""
		photo = ImageTk.PhotoImage(Image.open(imageFile))
		self.label.configure(image = photo, height=288) 
		self.label.image = photo
		
	
	def openRtpPort(self):
		"""Open RTP socket binded to a specified port."""
		# Create a new datagram socket to receive RTP packets from the server
		self.rtpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		
		# Set the timeout value of the socket to 0.5sec
		# self.rtpSocket.settimeout(0.5) #! Comentei este timeout
		
		try:
			# Bind the socket to the address using the RTP port
			addr = (self.addr, self.port)
			self.rtpSocket.bind(addr)
			print(f'\nBinded to {addr} \n')
		except:
			tkMessageBox.showwarning('Unable to Bind', 'Unable to bind PORT=%d' %self.rtpPort)

	def handler(self):
		"""Handler on explicitly closing the GUI window."""
		self.pauseMovie()
		if tkMessageBox.askokcancel("Quit?", "Are you sure you want to quit?"):
			self.exitClient()
		else: # When the user presses cancel, resume playing.
			self.playMovie()
   
