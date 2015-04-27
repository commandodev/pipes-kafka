{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ViewPatterns        #-}
module Pipes.Kafka where


import           Control.Lens
import           Control.Monad                  (forever, void)
import           Control.Monad.IO.Class         (liftIO)
import qualified Data.ByteString                as BS
import qualified Data.ByteString.Char8          as C8
import           Haskakafka                     (Kafka, KafkaError (..),
                                                 KafkaMessage (..),
                                                 KafkaOffset (..),
                                                 KafkaProduceMessage (..),
                                                 KafkaProducePartition (..),
                                                 KafkaTopic)
import qualified Haskakafka                     as HK
import qualified Haskakafka.InternalRdKafkaEnum as HK
import qualified Haskakafka.InternalSetup       as HK
import           Pipes                          (Consumer, Producer, runEffect,
                                                 (>->))
import qualified Pipes                          as P
import qualified Pipes.Prelude                  as P
import           Pipes.Safe                     (MonadCatch, MonadSafe, SafeT)
import qualified Pipes.Safe                     as PS

--------------------------------------------------------------------------------
type Topic = String

data ConnectOpts = ConnectOpts {
    _hostString     :: String
  , _topicName      :: String
  , _topicOverrides :: HK.ConfigOverrides
  , _kafkaOverrides :: HK.ConfigOverrides
  } deriving Show

makeLenses ''ConnectOpts

data MessageTimeout =
    After !Int
  | Never
  deriving (Show)

timeoutToInt :: MessageTimeout -> Int
timeoutToInt (Never) = (-1)
timeoutToInt (After i) = i


--------------------------------------------------------------------------------
kafkaSink
  :: (MonadSafe m)
  => ConnectOpts
  -> KafkaProducePartition
  -> Consumer BS.ByteString  m (Maybe KafkaError)
kafkaSink co partition = PS.bracket connect release publish
  where
    connect = liftIO $ do
      kafka <- HK.newKafka HK.RdKafkaProducer (co ^. kafkaOverrides)
      HK.addBrokers kafka (co ^. hostString)
      topic <- HK.newKafkaTopic kafka (co ^. topicName) (co ^. topicOverrides)
      return (kafka, topic)
    release (fst -> k) = liftIO $ HK.drainOutQueue k
    publish (snd -> t) = forever $ do
      m <- P.await
      liftIO $ HK.produceMessage t partition $ KafkaProduceMessage m

--------------------------------------------------------------------------------
kafkaSource
  :: forall m. (MonadSafe m)
  => ConnectOpts
  -> Int
  -> KafkaOffset
  -> MessageTimeout
  -> Producer KafkaMessage m ()
kafkaSource co partition offset to = PS.bracket connect release consume
  where
    connect = liftIO $ do
      kafka <- HK.newKafka HK.RdKafkaConsumer (co ^. kafkaOverrides)
      HK.addBrokers kafka (co ^. hostString)
      topic <- HK.newKafkaTopic kafka (co ^. topicName) (co ^. topicOverrides)
      HK.startConsuming topic partition offset
      return (kafka, topic)
    release (_kafka, topic) = liftIO $ HK.stopConsuming topic partition
    consume :: (Kafka, KafkaTopic) -> Producer KafkaMessage m ()
    consume (k, t) = do
      ret <- liftIO $ HK.consumeMessage t partition (timeoutToInt to)
      case ret of
        Left _err -> return ()
        Right m -> do
          P.yield m
          consume (k ,t)

--------------------------------------------------------------------------------
main :: IO ()
main = do
  PS.runSafeT $ runEffect nums
  PS.runSafeT $ runEffect $ P.for source $ \msg ->
    liftIO $ print msg
  where
    opts = ConnectOpts "localhost:9092" "testing" [] []
    sink = kafkaSink opts HK.KafkaUnassignedPartition
    nums = P.each [(1::Int)..5] >-> P.map (C8.pack . show) >-> (void sink)
    source = kafkaSource opts 0 HK.KafkaOffsetBeginning Never
