{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TemplateHaskell #-}
module Pipes.Kafka where

import Control.Lens
import Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Data.ByteString as BS
import           Haskakafka      (KafkaProducePartition (..), KafkaTopic)
import qualified Haskakafka      as HK
import qualified Haskakafka.InternalRdKafkaEnum      as HK
import qualified Haskakafka.InternalSetup      as HK
import           Pipes           (Consumer, Producer, runEffect)
import qualified Pipes           as P
import qualified Pipes.Prelude           as P
import           Pipes.Safe      (MonadSafe, SafeT, MonadCatch)
import qualified Pipes.Safe      as PS

type Topic = String

data ConnectOpts = ConnectOpts {
    _hostString :: String
  , _topicName :: String
  , _topicOverrides :: HK.ConfigOverrides
  , _kafkaOverrides :: HK.ConfigOverrides
  } deriving Show

makeLenses ''ConnectOpts

kafkaSink
  :: (MonadSafe m)
  => ConnectOpts
  -> KafkaProducePartition
  -> Consumer BS.ByteString (SafeT m) ()
kafkaSink co partition = PS.bracket connect publish release
  where
    connect = PS.liftBase $ do
      kafka <- HK.newKafka HK.RdKafkaProducer (co ^. kafkaOverrides)
      HK.addBrokers kafka (co ^. hostString)
      topic <- HK.newKafkaTopic kafka (co ^. topicName) (co ^. topicOverrides)
      return (kafka, topic)
    release (fst -> k) = PS.liftBase $ HK.drainOutQueue k
    publish (k, t) = _ -- P.mapM (P.liftIO $ HK.produceMessage t partition . HK.KafkaProduceMessage)
