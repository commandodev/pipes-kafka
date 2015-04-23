{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ViewPatterns        #-}
module Pipes.Kafka where


import           Control.Lens
import           Control.Monad                  (forever, void)
import           Control.Monad.IO.Class         (liftIO)
import qualified Data.ByteString                as BS
import qualified Data.ByteString.Char8          as C8
import           Haskakafka                     (KafkaProducePartition (..),
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

type Topic = String

data ConnectOpts = ConnectOpts {
    _hostString     :: String
  , _topicName      :: String
  , _topicOverrides :: HK.ConfigOverrides
  , _kafkaOverrides :: HK.ConfigOverrides
  } deriving Show

makeLenses ''ConnectOpts

kafkaSink
  :: (MonadSafe m)
  => ConnectOpts
  -> KafkaProducePartition
  -> Consumer BS.ByteString  m (Maybe HK.KafkaError)
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
      liftIO $ HK.produceMessage t partition $ HK.KafkaProduceMessage m
{-
  bracket
    (do
      kafka <- newKafka RdKafkaConsumer configOverrides
      addBrokers kafka brokerString
      topic <- newKafkaTopic kafka tName topicConfigOverrides
      startConsuming topic partition offset
      return (kafka, topic)
    )
    (\(_, topic) -> stopConsuming topic partition)
    (\(k, t) -> cb k t)
-}

kafkaSource
  :: forall m. (MonadSafe m)
  => ConnectOpts
  -> Int
  -> HK.KafkaOffset
  -> Producer HK.KafkaMessage m ()
kafkaSource co partition offset = PS.bracket connect release consume
  where
    connect = liftIO $ do
      kafka <- HK.newKafka HK.RdKafkaConsumer (co ^. kafkaOverrides)
      HK.addBrokers kafka (co ^. hostString)
      topic <- HK.newKafkaTopic kafka (co ^. topicName) (co ^. topicOverrides)
      HK.startConsuming topic partition offset
      return (kafka, topic)
    release (snd -> topic) = liftIO $ HK.stopConsuming topic partition
    consume :: (HK.Kafka, HK.KafkaTopic) -> Producer HK.KafkaMessage m ()
    consume (snd -> t) = forever $ do
      ret <- liftIO $ HK.consumeMessage t partition (-1)
      case ret of
        Left err -> return ()
        Right m -> P.yield m

main :: IO ()
main = do
  PS.runSafeT $ runEffect $ P.each [(1::Int)..5] >-> P.map (C8.pack . show) >-> (void sink)
  where
    opts = ConnectOpts "localhost:9092" "test" [] []
    sink = kafkaSink opts HK.KafkaUnassignedPartition
