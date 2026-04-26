package finance.valet

import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.TimeUnit

import android.app.ActivityManager
import android.content.{BroadcastReceiver, Context, Intent, IntentFilter}
import android.os.Build
import immortan.ConnectionProvider
import immortan.crypto.Tools._
import okhttp3.OkHttpClient
import org.torproject.jni.TorService

import scala.collection.JavaConverters._


class TorConnectionProvider(context: Context) extends ConnectionProvider {
  override val proxyAddress: Option[InetSocketAddress] = new InetSocketAddress("127.0.0.1", 9050).asSome

  private val proxy = new java.net.Proxy(java.net.Proxy.Type.SOCKS, proxyAddress.get)

  override val okHttpClient: OkHttpClient = (new OkHttpClient.Builder).proxy(proxy).connectTimeout(30, TimeUnit.SECONDS).build

  private val torServiceClassReference = classOf[TorService]

  @volatile private var pendingReceiver: Option[BroadcastReceiver] = None

  def doWhenReady(action: => Unit): Unit = synchronized {
    pendingReceiver.foreach(r => try context.unregisterReceiver(r) catch none)

    lazy val once: BroadcastReceiver = new BroadcastReceiver {
      override def onReceive(context: Context, intent: Intent): Unit =
        if (intent.getStringExtra(TorService.EXTRA_STATUS) == TorService.STATUS_ON) {
          try context.unregisterReceiver(once) catch none
          TorConnectionProvider.this.synchronized {
            if (pendingReceiver.contains(once)) pendingReceiver = None
          }
          action
        }
    }

    pendingReceiver = Some(once)
    val intentFilter = new IntentFilter(TorService.ACTION_STATUS)
    if (Build.VERSION.SDK_INT >= 33) context.registerReceiver(once, intentFilter, Context.RECEIVER_NOT_EXPORTED)
    else context.registerReceiver(once, intentFilter)
    context.startService(new Intent(context, torServiceClassReference))
  }

  override def getSocket: Socket = new Socket(proxy)

  override def notifyAppAvailable: Unit = {
    val services = context.getSystemService(Context.ACTIVITY_SERVICE).asInstanceOf[ActivityManager].getRunningServices(Integer.MAX_VALUE)
    val shouldReconnect = !services.asScala.exists(_.service.getClassName == torServiceClassReference.getName)
    if (shouldReconnect) doWhenReady(none)
  }
}
