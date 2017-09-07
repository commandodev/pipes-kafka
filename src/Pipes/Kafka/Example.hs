module Pipes.Kafka.Example where

import           Control.Monad          (void)
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Logger   (runStdoutLoggingT)
import qualified Data.ByteString.Char8  as C8
import           Data.Monoid            ((<>))
import           Kafka.Consumer         (BrokerAddress (..),
                                         ConsumerGroupId (..), OffsetReset (..),
                                         Timeout (..), TopicName (..),
                                         brokersList, consumerLogLevel, groupId,
                                         noAutoCommit, offsetReset, topics)
import qualified Kafka.Producer         as Producer
import           Kafka.Types            (KafkaLogLevel (..))
import           Pipes                  (runEffect, (>->))
import qualified Pipes                  as P
import           Pipes.Kafka
import qualified Pipes.Prelude          as P
import qualified Pipes.Safe             as PS

main :: IO ()
main = do
  PS.runSafeT $ runEffect nums
  runStdoutLoggingT $
    PS.runSafeT $ runEffect $ P.for source $ \msg -> liftIO $ print msg
  where
    sink = kafkaSink producerProps (TopicName "testing")
    producerProps =
      Producer.brokersList [BrokerAddress "localhost:9093"] <>
      Producer.logLevel KafkaLogDebug
    nums = P.each [(1 :: Int) .. 5] >-> P.map (C8.pack . show) >-> void sink
    source = kafkaSource consumerProps consumerSub (Timeout 1000)
    consumerProps =
      brokersList [BrokerAddress "localhost:9093"] <>
      groupId (ConsumerGroupId "consumer_example_group") <>
      noAutoCommit <>
      consumerLogLevel KafkaLogInfo
    consumerSub = topics [TopicName "testing"] <> offsetReset Earliest
