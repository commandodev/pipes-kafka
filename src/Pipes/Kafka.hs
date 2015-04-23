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
