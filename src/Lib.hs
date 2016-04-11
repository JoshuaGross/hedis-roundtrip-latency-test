{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Lib
    ( testHedis
    ) where

import           Control.Concurrent
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Loops
import           Data.ByteString.Char8
import           Data.Time
import           Database.Redis         as R
import           SlaveThread            (fork)

import           Data.Conduit           as C
import qualified Data.Conduit.List      as CL
import           Data.Conduit.TMChan
import           Data.IORef
import           Data.List              as L
import           Data.Maybe
import           GHC.Conc.Sync

mInSeconds :: Double
mInSeconds = 1000000

limitSeconds :: Double
limitSeconds = 10

-- bytes
msgSize :: Double
msgSize = 36

limit :: Double
limit = mInSeconds * limitSeconds

timeLatency :: Conduit ByteString IO Double
timeLatency = CL.mapM $ \msg -> do
  now <- getCurrentTime
  let msgTimeInFlight :: Maybe UTCTime = parseTimeM False defaultTimeLocale "%FT%T%q%z" $ unpack msg
  let difftime = fromMaybe 0 $ fmap (diffUTCTime now) msgTimeInFlight
  let difftime' = realToFrac difftime
  return difftime'

underLimit :: UTCTime -> Double -> IO Bool
underLimit start limit = do
  now <- getCurrentTime
  let difftime = (1000000 *) $ realToFrac $ diffUTCTime now start
  return $ difftime < limit

-- operate on triple of (total, min, max)
reduceLatency :: (Double, Double, Double, [Double]) -> Double -> (Double, Double, Double, [Double])
reduceLatency (total, mini, maxi, ss) a = (total + a, min mini a, max maxi a, a:ss)

displayResult :: (Double, Double, Double, [Double]) -> IO ()
displayResult (total, mini, maxi, dlist) = do
  let numWrites' = L.length dlist
  let numWrites :: Double = toEnum numWrites'
  let mid = numWrites' `div` 2
  let medianLatency = (L.sort dlist) !! (mid - 1)
  print $ "Test run for " ++ (show limitSeconds) ++ " seconds"
  print $ "Messages per second: " ++ (show $ numWrites / limitSeconds)
  print $ "Bytes per second: " ++ (show $ (numWrites * msgSize) / limitSeconds)
  print $ "Average latency seconds/message: " ++ (show $ total / numWrites)
  print $ "Median latency seconds/message: " ++ (show medianLatency)
  print $ "Max latency seconds/message: " ++ (show $ maxi)
  print $ "Min latency seconds/message: " ++ (show $ mini)

testHedis :: IO ()
testHedis = do
  conn <- R.connect defaultConnectInfo
  chan <- atomically $ (newTBMChan 32 :: STM (TBMChan ByteString))
  forkIO $ go conn chan
  result <- sourceTBMChan chan $$ timeLatency =$ CL.fold reduceLatency (0, 1000000, negate 1000000, [])
  displayResult result

  where

  go :: Connection -> TBMChan ByteString -> IO ()
  go conn chan = do
    -- redis producer
    fork $ do
      runRedis conn $ do
        start <- liftIO getCurrentTime
        publish "chan1" "hello"
        whileM (liftIO $ underLimit start limit) $ do
          now <- liftIO getCurrentTime
          publish "chan2" (pack $ formatTime defaultTimeLocale "%FT%T%q%z" now)
          liftIO $ threadDelay 100
        publish "chan2" "done"
        liftIO $ print "done producing"
        return ()

    -- redis consumer, conduit source
    runRedis conn $
      pubSub (psubscribe ["chan*"]) $ \msg -> do
        let msgTxt = msgMessage msg
        if msgTxt == "done"
          then do
            atomically $ closeTBMChan chan
            return $ punsubscribe ["chan*"]
          else do
            atomically $ writeTBMChan chan msgTxt
            return mempty
