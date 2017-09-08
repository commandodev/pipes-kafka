{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Pipes.Kafka where

import           Control.Concurrent        (threadDelay)
import           Control.Monad             (forever)
import           Control.Monad.Catch       (throwM)
import           Control.Monad.IO.Class    (MonadIO, liftIO)
import           Control.Monad.Logger      (MonadLogger, logDebug, logWarn)
import           Control.Monad.Trans.Class (lift)
import qualified Data.ByteString           as BS

import           Data.Text                 (pack)
import           Kafka.Consumer            (ConsumerProperties, ConsumerRecord,
                                            Subscription, Timeout (..),
                                            TopicName (..), closeConsumer,
                                            newConsumer, pollMessage)
import           Kafka.Producer            (ProducePartition (..),
                                            ProducerProperties,
                                            ProducerRecord (..),
                                            RdKafkaRespErrT (..), closeProducer,
                                            newProducer, produceMessage)
import           Kafka.Types               (KafkaError (..))
import           Pipes                     (Consumer, Producer)
import qualified Pipes                     as P

import           Pipes.Safe                (MonadSafe)
import qualified Pipes.Safe                as PS

kafkaSink ::
     (MonadIO m, MonadSafe m)
  => ProducerProperties
  -> TopicName
  -> Consumer BS.ByteString m ()
kafkaSink props topic = PS.bracket connect release publish
  where
    connect = do
      res <- liftIO $ newProducer props
      case res of
        Left err       -> liftIO $ throwM err
        Right producer -> pure producer
    release producer = liftIO $ closeProducer producer
    publish producer =
      forever $ do
        m <- P.await
        liftIO $ produceMessage producer $ mkMessage topic Nothing (Just m)
    mkMessage t k v =
      ProducerRecord
      {prTopic = t, prPartition = UnassignedPartition, prKey = k, prValue = v}

kafkaSource ::
     (MonadIO m, MonadSafe m, MonadLogger m)
  => ConsumerProperties
  -> Subscription
  -> Timeout
  -> Producer (ConsumerRecord (Maybe BS.ByteString) (Maybe BS.ByteString)) m ()
kafkaSource props subs timeout@(Timeout timeoutms) =
  PS.bracket connect release consume
  where
    connect = do
      res <- liftIO $ newConsumer props subs
      case res of
        Left err       -> liftIO $ throwM err
        Right consumer -> pure consumer
    release consumer = liftIO $ closeConsumer consumer
    consume consumer =
      forever $ do
        res <- liftIO $ pollMessage consumer timeout
        case res of
          Left (KafkaResponseError RdKafkaRespErrPartitionEof) ->
            waitForMessages timeoutms
          Left (KafkaResponseError RdKafkaRespErrTimedOut) -> waitForMessages 0
          Left err -> do
            lift . $(logWarn) . pack . show $ err
            waitForMessages timeoutms
          Right m -> P.yield m
    waitForMessages t = do
      lift $ $(logDebug) "waiting for new messages"
      liftIO . threadDelay $ 1000 * t

instance MonadLogger m => MonadLogger (PS.SafeT m)
