package ecg

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors

object ECGframe {


   val scale: Double = 1 / 200.0 // Mv
  //  val rootDir = "FileStore/fxo/ted/"


  val windowSize = 120 // 40 // 120 for 105s1
  var windowingF  =  new Array[Double](windowSize)   //differs of Array[Double](WINDOW) uahhh!!

  for ( i <- 0 until windowSize) {
    val y = Math.sin(Math.PI * i / (windowSize - 1.0))
    windowingF(i)=  y * y
  }

  def process ( wSize: Int, frame : Array[Double] , model:KMeansModel) : Array[Double] = {
    /**
      * frame:  frame items should be a scaled input signal
      */

    // Split frame in WINDOW size windows
    val frameLength = frame.length
    // should throws a exception if not
    assert( frameLength % wSize == 0 , " Received Frame length(" + frameLength +") should be multiple of window Size("+ wSize+")")

    // TODo: Instead collecting all windows it should be better just to store previous windows
    val signalWindows = scala.collection.mutable.ArrayBuffer.empty[Array[Double]]
    val codecWindows = scala.collection.mutable.ArrayBuffer.empty[Array[Double]]


    val ret = scala.collection.mutable.ArrayBuffer.empty[Double]
    val nWindows:Int = frameLength / wSize

    for ( i <- 0 until (nWindows * 2)-1) {   // we step 1/2 wSize
      val p = i*wSize/2    // half window
      val signalWindow = frame.slice(p, p+wSize  )

      // normalize
      val avg = signalWindow.reduce(_+_)/wSize
      val wNorm = Vectors.norm(Vectors.dense(signalWindow) , 2)

      var normWindow = signalWindow.map( i => (i-avg)/wNorm)

      // apply tap function
      var tapWindow = (normWindow, windowingF).zipped.map(_*_)   // multiple vector item by item


      /*var tapWindow = (signalWindow, windowingF).zipped.map(_*_)   // multiple vector item by item
      val wNorm = Vectors.norm(Vectors.dense(tapWindow) , 2)
      tapWindow = tapWindow.map( x => x/wNorm)
*/
      val nc = model.predict(Vectors.dense(tapWindow))

      val cluster = model.clusterCenters(nc)

      //val codecWindow = cluster.toArray.map(x => x*wNorm)   // scale to signal
      val codecWindow = cluster.toArray   // scale to signal

      signalWindows.append(signalWindow)
      codecWindows.append(codecWindow.toArray)
      // Coded windows has tapering function scale. get it of it using a trick sum two displacements halfs.
      // see properties or tapering function.
      // we produce half a window a time
      // and we have to arrange in some way first and last halfs
      var half1 = new Array[Double](wSize)
      var half2 = new Array[Double](wSize)
      if (i == 0) {
        half1 = codecWindows(i).slice(0,wSize/2)
        half2 = half1
        var r = (half1 , half2).zipped.map(_+_)   // sum halfs
        print(">")
      }
      else {
        print("*")
        half1= codecWindows(i-1).slice(wSize/2,wSize)
        half2= codecWindows(i).slice(0,wSize/2)
        var r = (half1 , half2).zipped.map(_+_)   // sum halfs
        // follow operation should be common but till now, we are ignoring frame boundaries.
        // denormalize
        //val codecWindow = cluster.toArray.map(x => x*wNorm)   // scale to signal

        ret ++= r.map( x => (x * wNorm) + avg )


      }

      // last half window
      if ( i == (nWindows*2) -2 ) {
        half1= codecWindows(i).slice(wSize/2,wSize)
        half2= half1
        print ("<")
        var r = (half1 , half2).zipped.map(_+_)   // sum halfs


      }

    }
    ret.toArray
  }

}
