{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE ViewPatterns        #-}
module Pipes.Kafka where

import           Control.Lens
import Control.Monad (forever)
import           Control.Monad.IO.Class         (liftIO)
import qualified Data.ByteString                as BS
import           Haskakafka                     (KafkaProducePartition (..),
                                                 KafkaTopic)
import qualified Haskakafka                     as HK
import qualified Haskakafka.InternalRdKafkaEnum as HK
import qualified Haskakafka.InternalSetup       as HK
import           Pipes                          (Consumer, Producer, runEffect)
import qualified Pipes                          as P
import qualified Pipes.Prelude                  as P
import           Pipes.Safe                     (MonadCatch, MonadSafe, SafeT)
import qualified Pipes.Safe                     as PS

type Topic = String
type Offset = Int

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
  -> Consumer BS.ByteString (SafeT m) (Maybe HK.KafkaError)
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
      P.liftIO $ HK.produceMessage t partition $ HK.KafkaProduceMessage m
{-

withKafkaConsumerSource

:: ConfigOverrides
config overrides for kafka. See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md. Use an empty list if you don't care.
-> ConfigOverrides
config overrides for topic. See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md. Use an empty list if you don't care.
-> String
broker string, e.g. localhost:9092
-> String
topic name
-> Int
partition to consume from. Locked until the function returns.
-> KafkaOffset
where to begin consuming in the partition.
-> (Kafka -> KafkaTopic -> IO a)
your cod, fed with Kafka and KafkaTopic instances for subsequent interaction.
-> IO a
-}

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
  :: (MonadSafe m)
  => ConnectOpts
  -> Int
  -> Offset
  -> Producer HK.KafkaMessage (SafeT m) ()
kafkaSource co partition offset = PS.bracket connect release consume
  where
    connect = liftIO $ do
      kafka <- HK.newKafka HK.RdKafkaConsumer (co ^. kafkaOverrides)
      HK.addBrokers kafka (co ^. hostString)
      topic <- HK.newKafkaTopic kafka (co ^. topicName) (co ^. topicOverrides)
      HK.startConsuming topic partition offset
      return (kafka, topic)
    release (kafka, topic) = liftIO $ HK.stopConsuming topic kafka
    consume (snd -> t) = forever $ do
      m <- P.liftIO $ HK.consumeMessage _ --t partition $ HK.KafkaProduceMessage m
      P.yield m

