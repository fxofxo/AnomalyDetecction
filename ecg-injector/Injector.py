import glob
import time
import os
import numpy as np
import json
from kafka import KafkaProducer

from time import sleep

class FrameInjector:
    def __init__(self, id, nEventsbyFrame,  dataDir,
                 fileExt=".dat", scale = 200,  isDatabricks=False ):
        """
        The events should be in binary file, each event is a integer of length evtlen

        nEventsbyFrame:   number  events that compound a frame

        dir:       Where to find source event files
        fileExt:   file extension
        bytesperSample:    2 by default (short) byte per event
        isDatabricks flag True if we are into a databrick notebook.
        """
        self.Id = id
        self.nEventsbyFrame = nEventsbyFrame
        self.dataDir = dataDir
        self.fileExt = fileExt
        self.scale = scale
        self.isDatabricks = isDatabricks
        self.topic = None
        self.servers = None
        self.fileList = None
        self.kafkaProducer = None
       # self.sampleTick = 1/100   # till now the DB is recorded with 100 samples per second
        if isDatabricks:
            self.dataDir = "/dbfs/" + self.dataDir
        print(self.dataDir)
        try:
            self.fileList = glob.glob(self.dataDir + "*" + ".dat")
        except Exception as e:
            print (e)

    def setKafkaTopic(self, topicName, servers):
        self.topic = topicName
        self.servers = servers
        self.kafkaProducer = KafkaProducer(bootstrap_servers=servers)

    def startInjection(self, nFrames = 0, delay = 1):
        """
        if nFrames is not 0 then it will stop when nframes limit is reached, or data is run out.
        """
        print(self.fileList)
        for fName in self.fileList:
            with open(fName, "rb") as f:
                print(fName)
                # format 212 . 12 bit per signal.
                # in arrithmia DB two signals per event so 3 byte per record.

                frameBytes = self.nEventsbyFrame * 3
                f.seek(0, os.SEEK_END)
                filelength = f.tell()
                f.seek(0, os.SEEK_SET)

                maxFrames = filelength // frameBytes
                if nFrames == 0:
                    nFrames = maxFrames
                else:
                    nFrames = min(maxFrames,nFrames)
                rest = filelength % frameBytes
                print ("Some events could be skipped: (%d)"%rest)
                print (nFrames)
                u4mask = 0xf0
                l4mask = 0x0f

                for i in range(0, nFrames):
                    buff = f.read(frameBytes)
                    s1 = []
                    s2 = []
                    for ridx in range(self.nEventsbyFrame):
                        p = ridx*3
                        r = buff[p:p+3]
                        a = ( ((r[1] & l4mask)) << 8 )+ r[0]
                        b = ( (r[1] & u4mask )<< 4) + r[2]
                        s1.append(a)
                        s2.append(b)
                    frame = s1
                    print(len(frame))
                    ts = time.time()
                    frameRef = i * self.nEventsbyFrame
                    dict = {"srcTs": time.time(),
                            "srcId": self.Id+fName,
                            "frameRef": frameRef,   # start ms of frame inside the file.
                            "data": frame,
                            "scale": self.scale}
                    jsonFrame = json.dumps(dict)   # i is added to debug streaming
                    ##print (jsonFrame)
                    self.kafkaProducer.send(self.topic, bytes( jsonFrame,"UTF-8"))
                    print("frame:%d",i)
                    sleep(delay)


if __name__ == "__main__":
    """ run injector instance test
    """
    if __name__ == "__main__":
        print( "runing")
        path = "./data/"
        topicName = "ecg-frame"
        servers = ['10.132.0.3:9091', '10.132.0.4:9091']
        #servers = ['10.132.0.3:9091']
        #servers = ['10.132.0.4:9091']
        windowSize = 120
        windowsbyFrame = 15    # is 5s for arrithmia db
        nEvents = windowSize * windowsbyFrame

        inj = FrameInjector("testInj2", nEventsbyFrame = nEvents, dataDir = path)
        #inj = FrameInjector("testInj2", 10, dataDir)
        inj.setKafkaTopic(topicName, servers)
        inj.startInjection(nFrames=0, delay = .1)

        ## The producer need some time to send the message an the call seem to be asyncronous.
        # so we need wait before quit the programs.
        r = input("wait to send message, please. Received)")

