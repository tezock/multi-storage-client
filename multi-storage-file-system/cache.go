package main

import (
	"container/list"
	"sync"
	"time"
)

const (
	pauseForCacheLineFillToPopulateCleanCacheLineLRU = 1 * time.Millisecond
)

// `fetch` is run in a goroutine for an allocated cacheLineStruct that
// is to be populated with a portion of the object's contents. Completion of
// the fetch operation is indicated by signaling as done the sync.WaitGroup
// in the cacheLineStruct itself.
func (cacheLine *cacheLineStruct) fetch() {
	var (
		backend        *backendStruct
		err            error
		inode          *inodeStruct
		ok             bool
		readFileInput  *readFileInputStruct
		readFileOutput *readFileOutputStruct
	)

	globals.Lock()

	inode, ok = globals.inodeMap[cacheLine.inodeNumber]
	if !ok {
		globals.logger.Printf("[WARN] [TODO] (*cacheLineStruct) fetch() needs to handle missing inodeStruct")
		cacheLine.state = CacheLineClean
		cacheLine.eTag = ""
		cacheLine.content = make([]byte, 0)
		globals.inboundCacheLineCount--
		cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
		cacheLine.notifyWaiters()
		globals.Unlock()
		return
	}

	backend = inode.backend

	readFileInput = &readFileInputStruct{
		filePath:        inode.objectPath,
		offsetCacheLine: cacheLine.lineNumber,
		ifMatch:         "",
	}

	globals.Unlock()

	readFileOutput, err = readFileWrapper(backend.context, readFileInput)
	if err != nil {
		globals.Lock()
		globals.logger.Printf("[WARN] [TODO] (*cacheLineStruct) fetch() needs to handle error reading cache line")
		cacheLine.state = CacheLineClean
		cacheLine.eTag = ""
		cacheLine.content = make([]byte, 0)
		globals.inboundCacheLineCount--
		cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
		cacheLine.notifyWaiters()
		globals.Unlock()
		return
	}

	globals.Lock()
	cacheLine.state = CacheLineClean
	cacheLine.eTag = readFileOutput.eTag
	cacheLine.content = readFileOutput.buf
	globals.inboundCacheLineCount--
	cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
	cacheLine.notifyWaiters()
	globals.Unlock()
}

// `touch` is called while globals.Lock() is held to update the placement of
// a cacheLineStruct on globals.{clean|dirty}CacheLineLRU if it is currently
// on either.
func (cacheLine *cacheLineStruct) touch() {
	switch cacheLine.state {
	case CacheLineInbound:
		// Nothing to do here
	case CacheLineClean:
		globals.cleanCacheLineLRU.Remove(cacheLine.listElement)
		cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
	case CacheLineOutbound:
		// Nothing to do here
	case CacheLineDirty:
		globals.dirtyCacheLineLRU.Remove(cacheLine.listElement)
		cacheLine.listElement = globals.dirtyCacheLineLRU.PushBack(cacheLine)
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] cacheLine.state (%v) unexpected", cacheLine.state)
	}
}

// `notifyWaiters` is called while holding glohbals.Lock() to notify all those
// in the .waiters slice awaiting a state change of this cacheLine. Upon return,
// // the .waiters slice will be emptied.
func (cacheLine *cacheLineStruct) notifyWaiters() {
	var (
		waiter *sync.WaitGroup
	)

	for _, waiter = range cacheLine.waiters {
		waiter.Done()
	}

	cacheLine.waiters = make([]*sync.WaitGroup, 0, 1)
}

// `cacheFull` reports whether or not the cache is currently at (or
// exceeding) the configured cap on cache lines. This call must be
// made while holding the global lock.
func cacheFull() (isFull bool) {
	isFull = (globals.inboundCacheLineCount + uint64(globals.cleanCacheLineLRU.Len())) >= globals.config.cacheLines
	return
}

func cachePrefetch(inodeNumber, currentCacheLineNumber uint64) {
	var (
		cacheLine                       *cacheLineStruct
		cacheLineNumberMaxInBackend     uint64
		cacheLinesToPotentiallyPrefetch uint64
		cachePruneNeeded                bool
		inode                           *inodeStruct
		ok                              bool
		prefetchCacheLinesIssued        uint64
		prefetchCacheLineNumber         uint64
		prefetchCacheLineNumberMax      uint64
		prefetchCacheLineNumberMin      uint64
	)

	defer func() {
		if (inode != nil) && (prefetchCacheLinesIssued != 0) {
			globals.fissionMetrics.ReadCachePrefetches.Add(float64(prefetchCacheLinesIssued))

			if inode.backend != nil {
				inode.backend.fissionMetrics.ReadCachePrefetches.Add(float64(prefetchCacheLinesIssued))
			}
		}

		globals.Unlock()

		if cachePruneNeeded {
			cachePrune()
		}
	}()

	globals.Lock()

	inode, ok = globals.inodeMap[inodeNumber]
	if !ok {
		inode = nil
		return
	}

	cacheLineNumberMaxInBackend = ((inode.sizeInBackend + globals.config.cacheLineSize - 1) / globals.config.cacheLineSize) - 1

	if cacheLineNumberMaxInBackend >= (currentCacheLineNumber + globals.config.cacheLinesToPrefetch) {
		cacheLinesToPotentiallyPrefetch = globals.config.cacheLinesToPrefetch
	} else {
		cacheLinesToPotentiallyPrefetch = cacheLineNumberMaxInBackend - currentCacheLineNumber
	}

	if cacheLinesToPotentiallyPrefetch == 0 {
		return
	}

	prefetchCacheLineNumberMin = currentCacheLineNumber + 1
	prefetchCacheLineNumberMax = prefetchCacheLineNumberMin + cacheLinesToPotentiallyPrefetch - 1

	for prefetchCacheLineNumber = prefetchCacheLineNumberMin; prefetchCacheLineNumber <= prefetchCacheLineNumberMax; prefetchCacheLineNumber++ {
		_, ok = inode.cache[prefetchCacheLineNumber]
		if !ok {
			cacheLine = &cacheLineStruct{
				state:       CacheLineInbound,
				waiters:     make([]*sync.WaitGroup, 0, 1),
				inodeNumber: inodeNumber,
				lineNumber:  prefetchCacheLineNumber,
			}

			inode.cache[prefetchCacheLineNumber] = cacheLine

			globals.inboundCacheLineCount++

			go cacheLine.fetch()

			prefetchCacheLinesIssued++
		}
	}

	cachePruneNeeded = cacheFull()
}

// `cachePrune` is called to immediately force the cache to make room
// for a cacheLineStruct to be added. As this opereation will possibly
// block, callers should not hold any locks.
func cachePrune() {
	var (
		cacheLineToEvict *cacheLineStruct
		inode            *inodeStruct
		listElement      *list.Element
		ok               bool
	)

	globals.Lock()

	if !cacheFull() {
		globals.Unlock()
		return
	}

	listElement = globals.cleanCacheLineLRU.Front()

	if listElement == nil {
		// [TODO] Non-ideal simple pause/retry awaiting an element on cleanCacheLineLRU to evict

		globals.Unlock()

		time.Sleep(pauseForCacheLineFillToPopulateCleanCacheLineLRU)

		return
	}

	_ = globals.cleanCacheLineLRU.Remove(listElement)

	cacheLineToEvict, ok = listElement.Value.(*cacheLineStruct)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] listElement.Value.(*cacheLineStruct) returned !ok")
	}

	inode, ok = globals.inodeMap[cacheLineToEvict.inodeNumber]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap[cacheLineToEvict.inodeNumber] returned !ok [cachePrune()]")
	}

	_, ok = inode.cache[cacheLineToEvict.lineNumber]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] inode.cache[cacheLineToEvict.lineNumber] returned !ok")
	}

	delete(inode.cache, cacheLineToEvict.lineNumber)

	globals.Unlock()

	return
}
