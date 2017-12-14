import glob
import time
import numpy as np
import json
from kafka import KafkaProducer

from time import sleep

class FrameInjector:
    def __init__(self, id, nEventsbyFrame, evtTime, dataDir,
                 fileExt=".dat", evtLen=2, isDatabricks=False ):
        """
        The events should be in binary file, each event is a integer of length evtlen

        nEventsbyFrame:   number f events that compound a frame
        evtTime:   Period (ms) of simple event ( evtTime * nFrames = FramePeriod)
        dir:       Where to find source event files
        fileExt:   file extension
        evtLen:    2 by default (short)
        isDatabricks flag True if we are into a databrick notebook.
        """
        self.Id = id
        self.nEventsbyFrame = nEventsbyFrame
        self.evtTime = evtTime
        self.dataDir = dataDir
        self.fileExt = fileExt
        self.evtLen = evtLen
        self.isDatabricks = isDatabricks
        self.topic = None
        self.servers = None
        self.fileList = None
        self.kafkaProducer = None
        self.sampleTick = 1/100   # till now the DB is recorded with 100 samples per second
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

    def startInjection(self, nFrames=0):
        """
        if nFrames is not 0 then it will stop when nframes limit is reached, or data is run out.
        """
        print(self.fileList)
        for fName in self.fileList:
            with open(fName, "rb") as f:
                print(fName)
                events = np.fromfile(f, dtype=">i2")   # ">i2" big endian short. Read all file in memory, be carefull.
                maxFrames = len(events)/self.nEventsbyFrame
                if nFrames == 0:
                    nFrames = maxFrames
                else:
                    nFrames = min(maxFrames,nFrames)
                rest = len(events) % self.nEventsbyFrame
                print ("Some events will be skipped: (%d)"%rest)
                print (nFrames)
                for i in range(0, nFrames):
                    frame = events[i * self.nEventsbyFrame: (i+1)*self.nEventsbyFrame]
                    ts = time.time()
                    frameTref = i * self.sampleTick * self.nEventsbyFrame
                    dict = {"srcTs": time.time(),
                            "srcId": self.Id+fName,
                            "seqTref": frameTref,   # start ms of frame inside the file.
                            "data": frame.tolist()}
                    jsonFrame = json.dumps(dict)   # i is added to debug streaming
                    print (jsonFrame)
                    self.kafkaProducer.send(self.topic, bytes( jsonFrame,"UTF-8"))
                    sleep(1)

if __name__ == "__main__":
    """ run injector instance test
    """
    if __name__ == "__main__":
        print( "runing")
        dataDir = "./data/"
        topicName = "ecg"
        servers = ['10.132.0.3:9091', '10.132.0.4:9091']
        #servers = ['10.132.0.3:9091']
        #servers = ['10.132.0.4:9091']

        inj = FrameInjector("testInj2", 6000, 1000, dataDir)
        #inj = FrameInjector("testInj2", 10, 1000, dataDir)
        inj.setKafkaTopic(topicName, servers)
        inj.startInjection(5)
        ## The producer need some time to send the message an the call seem to be asyncronous.
        # so we need wait before quit the programs.
        r = input("wait to send message, please. Received)")

