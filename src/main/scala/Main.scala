import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import alpaca.Alpaca
import alpaca.dto.request.OrderRequest
import com.typesafe.scalalogging.Logger
import io.circe.{Json, parser}
import io.circe.generic.auto._
import io.circe._
import io.circe.parser._

import scala.collection.mutable

object logger {
  val logger = Logger("Main")
}

class MainRunnable extends Runnable {
  override def run(): Unit = {

    val alpaca = Alpaca()

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val streamingClient = alpaca.getStream()

    val listOfStocks = List("AAPL", "T", "SNAP")
    val maxShares = 500

    val quoteList = listOfStocks.map(stock => s"Q.$stock")
    val tradeList = listOfStocks.map(stock => s"T.$stock")
    val quoteMap =
      mutable.Map(
        listOfStocks
          .flatMap(stock => mutable.Map(stock -> Quote()))
          .toMap
          .toSeq: _*)
    val positionMap =
      mutable.Map(
        listOfStocks
          .flatMap(stock => mutable.Map(stock -> Position()))
          .toMap
          .toSeq: _*)

    val comboList: List[String] = quoteList ::: tradeList
    val comboMap = streamingClient.sub(comboList)
    val quoteStreams = comboMap.filter(x => x._1.contains("Q."))
    val tradeStreams = comboMap.filter(x => x._1.contains("T."))
    val tradeUpdateSteam = streamingClient.subscribeAlpaca("trade_updates")

    quoteStreams.foreach { quoteTuple =>
      quoteTuple._2._2.runWith(Sink.foreach(x => {
        val sq = parser.decode[StockQuote](x.data).getOrElse(null)
        val qt = quoteMap(sq.sym).update(sq)
        quoteMap(sq.sym) = qt
      }))
    }

    tradeUpdateSteam._2.runWith(Sink.foreach { tradeUpdateString =>
      val cursor = parse(tradeUpdateString.data).getOrElse(Json.Null).hcursor
      val tradeData = cursor.downField("data")
      val order = tradeData.downField("order")
      val symbol = order.get[String]("symbol").getOrElse(null)
      if (symbol != null) {
        var position = positionMap(symbol)
        val event = tradeData.get[String]("event").toOption.get
        val orderId = order.get[String]("id").getOrElse(null)
        val side = order.get[String]("side").getOrElse(null)
        val filledQty = order.get[Int]("filled_qty").getOrElse(0)
        if ("fill".equalsIgnoreCase(event)) {
          if ("buy".equalsIgnoreCase(side)) {
            position = position.updateTotalShares(filledQty)
          } else {
            position = position.updateTotalShares(filledQty * -1)
          }
          position = position.removePendingOrder(orderId, side)
          positionMap(symbol) = position
        } else if ("partial_fill"
                     .equalsIgnoreCase(event)) {
          position = position.updateFilledAmount(orderId, filledQty, side)
          positionMap(symbol) = position
        } else if ("canceled".equalsIgnoreCase(event) || "rejected"
                     .equalsIgnoreCase(event)) {
          position = position.removePendingOrder(orderId, side)
          positionMap(symbol) = position
        }
      }

    })

    tradeStreams.foreach { tradeTuple =>
      tradeTuple._2._2.runWith(Sink.foreach(message => {
        val stockTrade = parser.decode[StockTrade](message.data).getOrElse(null)
        val quote = quoteMap(stockTrade.sym)
        var position = positionMap(stockTrade.sym)
        if (!quote.traded && stockTrade.t >= quote.time + 50) {
          if (stockTrade.s >= 100) {
            if (stockTrade.p == quote.ask && quote.bidSize > quote.askSize * 1.8 && (position.totalShares + position.pendingBuyShares) < maxShares - 100) {
              val order = alpaca
                .placeOrder(
                  OrderRequest(stockTrade.sym,
                               "100",
                               "buy",
                               "limit",
                               "day",
                               Some(quote.ask.toString)))
                .unsafeRunSync()
              alpaca.cancelOrder(order.id)
              logger.logger.info(s"Buy at : ${quote.ask}")
              position = position.updatePendingBuyShares(100)
              position = position.createFilledAmount(order.id, 0)
              position = position.createFilledAmount(order.id, 0)
              quoteMap(stockTrade.sym) = quote.copy(traded = true)
              positionMap(stockTrade.sym) = position
            } else if (stockTrade.p == quote.bid && quote.askSize > (quote.bidSize * 1.8) && (position.totalShares - position.pendingSellShares) >= 100) {
              val order = alpaca
                .placeOrder(
                  OrderRequest(stockTrade.sym,
                               "100",
                               "sell",
                               "limit",
                               "day",
                               Some(quote.bid.toString)))
                .unsafeRunSync()
              alpaca.cancelOrder(order.id)
              logger.logger.info(s"Sell at : ${quote.bid}")
              position = position.updatePendingSellShares(100)
              position = position.removeFilledAmount(order.id)
              quoteMap(stockTrade.sym) = quote.copy(traded = true)
              positionMap(stockTrade.sym) = position
            }
          }
        }
      }))

    }

    while (true) {}
  }
}

object Main extends App {
  val mt = new Thread(new MainRunnable())
  mt.start()

}
case class StockQuote(
    ev: Option[String], // Event Type
    sym: String, // Symbol Ticker
    bx: Int, // Bix Exchange ID
    bp: Double, // Bid Price
    bs: Int, // Bid Size
    ax: Int, // Ask Exchange ID
    ap: Double, // Ask Price
    as: Int, // Ask Size
    c: Int, // Quote Condition
    t: Long // Quote Timestamp ( Unix MS )
)

case class StockTrade(ev: Option[String],
                      sym: String,
                      x: Int,
                      p: Double,
                      s: Int,
                      c: Array[Int],
                      t: Long)

case class Quote(
    prevBid: Double = 0,
    prevAsk: Double = 0,
    prevSpread: Double = 0,
    bid: Double = 0,
    ask: Double = 0,
    bidSize: Int = 0,
    askSize: Int = 0,
    spread: Double = 0,
    traded: Boolean = false,
    levelCt: Int = 1,
    time: Long = 0
) {
  def reset(): Quote = {
    copy(traded = false, levelCt = this.levelCt + 1)
  }

  def update(stockQuote: StockQuote): Quote = {
    var tempCopy = copy(bidSize = stockQuote.bs, askSize = stockQuote.as)
    if (tempCopy.bid != stockQuote.bp && tempCopy.ask != stockQuote.ap && round(
          stockQuote.ap - stockQuote.bp,
          2) == .01) {

      val prevBidCopy = tempCopy.bid
      val prevAskCopy = tempCopy.ask
      val prevSpreadCopy = round(prevAskCopy - prevBidCopy, 3)
      val spreadCopy = round(stockQuote.ap - stockQuote.bp, 3)
      tempCopy = tempCopy.copy(prevBid = prevBidCopy,
                               prevAsk = prevAskCopy,
                               prevSpread = prevSpreadCopy,
                               spread = spreadCopy,
                               ask = stockQuote.ap,
                               bid = stockQuote.bp,
                               time = stockQuote.t)

      logger.logger.info(
        s"Level change, ${tempCopy.prevBid}, ${tempCopy.prevAsk}, ${tempCopy.prevSpread}, ${tempCopy.bid}, ${tempCopy.ask}, ${tempCopy.spread}")

      if (tempCopy.prevSpread == 0.01) {
        tempCopy = tempCopy.reset()
      }

    }
    tempCopy
  }

  private def round(value: Double, places: Int): Double = {
    val scale = Math.pow(10, places)
    Math.round(value * scale) / scale
  }
}

case class Position(pendingBuyShares: Int = 0,
                    pendingSellShares: Int = 0,
                    totalShares: Int = 0,
                    ordersFilledAmount: Map[String, Int] = Map()) {

  def updatePendingBuyShares(quantity: Int): Position = {
    copy(pendingBuyShares = this.pendingBuyShares + quantity)
  }

  def updatePendingSellShares(quantity: Int): Position = {
    copy(pendingSellShares = this.pendingSellShares + quantity)
  }

  def createFilledAmount(orderID: String, amount: Int): Position = {
    copy(ordersFilledAmount = ordersFilledAmount + (orderID -> amount))
  }

  def removeFilledAmount(orderID: String): Position = {
    copy(ordersFilledAmount = ordersFilledAmount + (orderID -> 0))
  }

  def updateFilledAmount(orderID: String,
                         newAmount: Int,
                         side: String): Position = {
    val oldAmount = ordersFilledAmount(orderID)
    if (newAmount > oldAmount) {
      val tempCopy = if ("buy".equalsIgnoreCase(side)) {
        updatePendingBuyShares(oldAmount - newAmount)
          .updateTotalShares(oldAmount - newAmount)
      } else {
        updatePendingSellShares(oldAmount - newAmount)
          .updateTotalShares(oldAmount - newAmount)
      }
      tempCopy.copy(
        ordersFilledAmount = ordersFilledAmount + (orderID -> newAmount))
    } else {
      this
    }
  }

  def removePendingOrder(orderID: String, side: String): Position = {
    val oldAmount = ordersFilledAmount(orderID)
    var tempCopy = if ("buy".equalsIgnoreCase(side)) {
      updatePendingBuyShares(oldAmount - 100)
    } else {
      updatePendingSellShares(oldAmount - 100)
    }
    tempCopy.copy(ordersFilledAmount = ordersFilledAmount - orderID)
  }

  def updateTotalShares(quantity: Int): Position = {
    copy(totalShares = totalShares + quantity)
  }

}
