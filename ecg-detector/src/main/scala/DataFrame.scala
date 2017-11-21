/**
  * Created by fsainz. felix.sainz@gmail.com
  *
  * SUMMARY:
  *
  */
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.LabeledPoint


class DataFrame{

  // CLass primary constructor
  // val STEP = 2
  // val SAMPLES = 200000
  // val scale: Double = 1 / 200.0 // Mv
  //  val rootDir = "FileStore/fxo/ted/"

  def process ( wSize: Int, frame : Array[Double] , model:KMeansModel) : Array[Double] = {

    // Split frame in WINDOW size windows
    val frameLength = frame.length

    assert( frameLength % wSize == 0 )  // should throws a exception if not

    val signalWindows = scala.collection.mutable.ArrayBuffer.empty[Array[Double]]
    val codecWindows = scala.collection.mutable.ArrayBuffer.empty[Array[Double]]
    val ret = scala.collection.mutable.ArrayBuffer.empty[Double]
    val nWindows:Int = frameLength / wSize

    for ( i <- 0 until (nWindows * 2)-1) {   // we step 1/2 wSize
      var p = i*wSize/2
      var signalWindow = frame.slice(p, p+wSize  )

      var tapWindow = (signalWindow,DataFrame.windowingF).zipped.map(_*_)   // multiple vector item by item
      val wNorm = Vectors.norm(Vectors.dense(tapWindow) , 2)
      tapWindow = tapWindow.map( x => x/wNorm)

      val nc = model.predict(Vectors.dense(tapWindow))

      val cluster = model.clusterCenters(nc)

      val codecWindow = cluster.toArray.map(x => x*wNorm)   // scale to signal

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
      }
      else {
        print(","+i+",")
        half1= codecWindows(i-1).slice(wSize/2,wSize)
        half2= codecWindows(i).slice(0,wSize/2)

      }
      var r = (half1 , half2).zipped.map(_+_)   // sum halfs
      ret ++= r
      // last half window
      if ( i == (nWindows*2) -2 ) {
        half1= codecWindows(i).slice(wSize/2,wSize)
        half2= half1
        print ("*************LAST + " + i)
        r = (half1 , half2).zipped.map(_+_)   // sum halfs
        ret ++= r
      }
    }
    ret.toArray
  }
}
object DataFrame {
  val WINDOW = 32
  var windowingF  =  new Array[Double](WINDOW)   //differs of Array[Double](WINDOW) uahhh!!

  for ( i <- 0 until WINDOW) {
    val y = Math.sin(Math.PI * i / (WINDOW - 1.0));
    windowingF(i)=  y * y
  }
}
