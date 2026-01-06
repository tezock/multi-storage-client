package main

import (
	"container/list"
	"context"
	"syscall"
	"time"
)

// `initFS` initializes the root of the FUSE file system.
func initFS() {
	var (
		timeNow time.Time
	)

	globals.Lock()

	timeNow = time.Now()

	globals.lastNonce = FUSERootDirInodeNumber

	globals.inode = &inodeStruct{
		inodeNumber:       FUSERootDirInodeNumber,
		inodeType:         FUSERootDir,
		backend:           nil,
		parentInodeNumber: FUSERootDirInodeNumber,
		isVirt:            true,
		objectPath:        "",
		basename:          "",
		sizeInBackend:     0,
		sizeInMemory:      0,
		eTag:              "",
		mode:              uint32(syscall.S_IFDIR | globals.config.dirPerm),
		mTime:             timeNow,
		xTime:             time.Time{},
		listElement:       nil,
		fhMap:             make(map[uint64]*fhStruct),
		physChildInodeMap: newStringToUint64Map(PhysChildInodeMap),
		virtChildInodeMap: newStringToUint64Map(VirtChildInodeMap),
		cache:             nil,
	}

	globals.inodeMap = make(map[uint64]*inodeStruct)

	_ = globals.inode.virtChildInodeMap.Put(DotDirEntryBasename, FUSERootDirInodeNumber)
	_ = globals.inode.virtChildInodeMap.Put(DotDotDirEntryBasename, FUSERootDirInodeNumber)

	globals.inodeMap[FUSERootDirInodeNumber] = globals.inode

	globals.inodeEvictionLRU = newTimeToUint64Queue(InodeEvictionLRU)

	globals.inodeEvictorContext, globals.inodeEvictorCancelFunc = context.WithCancel(context.Background())
	globals.inodeEvictorWaitGroup.Go(inodeEvictor)

	globals.inboundCacheLineCount = 0
	globals.cleanCacheLineLRU = list.New()
	globals.outboundCacheLineCount = 0
	globals.dirtyCacheLineLRU = list.New()

	globals.fissionMetrics = newFissionMetrics()
	globals.backendMetrics = newBackendMetrics()

	globals.Unlock()
}

// `drainFS` awaits all backend/asynchronous traffic to complete before
func drainFS() {
	var (
		dirName string
		backend *backendStruct
	)

	globals.inodeEvictorCancelFunc()
	globals.inodeEvictorWaitGroup.Wait()

	globals.Lock()

	for dirName, backend = range globals.config.backends {
		globals.backendsToUnmount[dirName] = backend
	}

	processToUnmountListAlreadyLocked()

	globals.Unlock()
}

// `processToMountList` creates a backend subdirectory of the FUSE
// file system's root directory that maps to each backend on the
// globals.backendsToMount list.
func processToMountList() {
	var (
		backend *backendStruct
		dirName string
		err     error
		ok      bool
		timeNow time.Time
	)

	globals.Lock()

	timeNow = time.Now()

	for dirName, backend = range globals.backendsToMount {
		delete(globals.backendsToMount, dirName)

		err = backend.setupContext()
		if err != nil {
			globals.logger.Printf("[WARN] unable to setup backend context: %s (err: %v) [skipping]", dirName, err)
			continue
		}

		backend.inode = &inodeStruct{
			inodeNumber:       fetchNonce(),
			inodeType:         BackendRootDir,
			backend:           backend,
			parentInodeNumber: FUSERootDirInodeNumber,
			isVirt:            true,
			objectPath:        "",
			basename:          dirName,
			sizeInBackend:     0,
			sizeInMemory:      0,
			eTag:              "",
			mode:              uint32(syscall.S_IFDIR | backend.dirPerm),
			mTime:             timeNow,
			xTime:             time.Time{},
			listElement:       nil,
			fhMap:             make(map[uint64]*fhStruct),
			physChildInodeMap: newStringToUint64Map(PhysChildInodeMap),
			virtChildInodeMap: newStringToUint64Map(VirtChildInodeMap),
			cache:             nil,
		}

		ok = globals.inode.virtChildInodeMap.Put(backend.dirName, backend.inode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] put of \"%s\" into backend.inode.virtChildInodeMap returned !ok", backend.dirName)
		}

		_ = backend.inode.virtChildInodeMap.Put(DotDirEntryBasename, backend.inode.inodeNumber)
		_ = backend.inode.virtChildInodeMap.Put(DotDotDirEntryBasename, FUSERootDirInodeNumber)

		globals.inodeMap[backend.inode.inodeNumber] = backend.inode

		backend.fissionMetrics = newFissionMetrics()
		backend.backendMetrics = newBackendMetrics()

		backend.mounted = true

		globals.config.backends[dirName] = backend
	}

	globals.Unlock()
}

// `processToUnmountList` is called to remove each backend subdirectory of the FUSE
// file system's root directory found on the globals.backendsToUnmount list.
func processToUnmountList() {
	globals.Lock()
	processToUnmountListAlreadyLocked()
	globals.Unlock()
}

// `processToUnmountListAlreadyLocked` is called while globals.Lock() is held to
// remove each backend subdirectory of the FUSE file system's root directory found
// on the globals.backendsToUnmount list.
func processToUnmountListAlreadyLocked() {
	var (
		backend *backendStruct
		dirName string
		ok      bool
	)

	for dirName, backend = range globals.backendsToUnmount {
		delete(globals.backendsToUnmount, dirName)

		backend.inode.emptyChildInodes()

		ok = globals.inode.virtChildInodeMap.DeleteByKey(backend.dirName)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] delete of \"%s\" from globals.inode.virtChildInodeMap returned !ok", backend.dirName)
		}

		delete(globals.inodeMap, backend.inode.inodeNumber)

		backend.mounted = false

		delete(globals.config.backends, dirName)
	}
}

// `emptyChildInodes` is called to remove all child inodes.
func (parentInode *inodeStruct) emptyChildInodes() {
	var (
		childInode         *inodeStruct
		childInodeBasename string
		childInodeNumber   uint64
		ok                 bool
	)

	for {
		childInodeBasename, childInodeNumber, ok = parentInode.physChildInodeMap.GetByIndex(0)
		if !ok {
			break
		}

		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [case physChildInodeMap]")
		}

		if childInode.inodeType == PseudoDir {
			childInode.emptyChildInodes()
		}

		if childInode.listElement != nil {
			globals.inodeEvictionLRU.Remove(childInode.xTime, childInode.listElement)
		}

		delete(globals.inodeMap, childInodeNumber)

		ok = parentInode.physChildInodeMap.DeleteByKey(childInodeBasename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.DeleteByKey(childInodeBasename) returned !ok")
		}
	}

	for {
		childInodeBasename, childInodeNumber, ok = parentInode.virtChildInodeMap.GetByIndex(0)
		if !ok {
			break
		}

		if (childInodeBasename != DotDirEntryBasename) && (childInodeBasename != DotDotDirEntryBasename) {
			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [case virtChildInodeMap]")
			}

			if childInode.inodeType == PseudoDir {
				childInode.emptyChildInodes()
			}

			if childInode.listElement != nil {
				globals.inodeEvictionLRU.Remove(childInode.xTime, childInode.listElement)
			}

			delete(globals.inodeMap, childInodeNumber)
		}

		ok = parentInode.virtChildInodeMap.DeleteByKey(childInodeBasename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInodeBasename) returned !ok")
		}
	}
}

// `convertToPhysInodeIfNecessary` is called while globals.Lock() is held to convert
// the supplied inode from "virt" to "phys" if necessary. It is the caller's responsibility
// to ensure that the directory path leading down to this now assuredly "phys" inode has
// already been ensured to be "phys".
func (childInode *inodeStruct) convertToPhysInodeIfNecessary() {
	var (
		ok          bool
		parentInode *inodeStruct
	)

	if !childInode.isVirt || ((childInode.inodeType != FileObject) && (childInode.inodeType != PseudoDir)) {
		return
	}

	if childInode.listElement != nil {
		globals.inodeEvictionLRU.Remove(childInode.xTime, childInode.listElement)
		childInode.xTime = time.Time{}
		childInode.listElement = nil
	}

	childInode.isVirt = false

	parentInode, ok = globals.inodeMap[childInode.parentInodeNumber]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap[childInode.parentInodeNumber] returned !ok")
	}

	ok = parentInode.virtChildInodeMap.DeleteByKey(childInode.basename)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInode.basename) returned !ok")
	}

	ok = parentInode.physChildInodeMap.Put(childInode.basename, childInode.inodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.Put(childInode.basename, childInode.inodeNumber) returned !ok")
	}

	childInode.touch(nil)
}

// `createPseudoDirInode` is called while globals.Lock() is held to create a new PsuedoDir inodeStruct.
func (parentInode *inodeStruct) createPseudoDirInode(isVirt bool, basename string) (pseudoDirInode *inodeStruct) {
	var (
		ok      bool
		timeNow = time.Now()
	)

	pseudoDirInode = &inodeStruct{
		inodeNumber:       fetchNonce(),
		inodeType:         PseudoDir,
		backend:           parentInode.backend,
		parentInodeNumber: parentInode.inodeNumber,
		isVirt:            isVirt,
		// objectPath: filled in below
		basename:          basename,
		sizeInBackend:     0,
		sizeInMemory:      0,
		eTag:              "",
		mode:              uint32(syscall.S_IFDIR | parentInode.backend.dirPerm),
		mTime:             timeNow,
		xTime:             time.Time{},
		listElement:       nil,
		fhMap:             make(map[uint64]*fhStruct),
		physChildInodeMap: newStringToUint64Map(PhysChildInodeMap),
		virtChildInodeMap: newStringToUint64Map(VirtChildInodeMap),
		cache:             nil,
	}

	if parentInode.objectPath == "" {
		pseudoDirInode.objectPath = basename + "/"
	} else {
		pseudoDirInode.objectPath = parentInode.objectPath + basename + "/"
	}

	_ = pseudoDirInode.virtChildInodeMap.Put(DotDirEntryBasename, pseudoDirInode.inodeNumber)
	_ = pseudoDirInode.virtChildInodeMap.Put(DotDotDirEntryBasename, parentInode.inodeNumber)

	if isVirt {
		ok = parentInode.virtChildInodeMap.Put(basename, pseudoDirInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.Put(basename, pseudoDirInode.inodeNumber) returned !ok")
		}
	} else {
		ok = parentInode.physChildInodeMap.Put(basename, pseudoDirInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.Put(basename, pseudoDirInode.inodeNumber) returned !ok")
		}
	}

	globals.inodeMap[pseudoDirInode.inodeNumber] = pseudoDirInode

	pseudoDirInode.touch(nil)

	return
}

// `createFileObjectInode` is called while globals.Lock() is held to create a new FileObject inodeStruct.
func (parentInode *inodeStruct) createFileObjectInode(isVirt bool, basename string, size uint64, eTag string, mTime time.Time) (fileObjectInode *inodeStruct) {
	var (
		ok bool
	)

	fileObjectInode = &inodeStruct{
		inodeNumber:       fetchNonce(),
		inodeType:         FileObject,
		backend:           parentInode.backend,
		parentInodeNumber: parentInode.inodeNumber,
		isVirt:            isVirt,
		// objectPath: filled in below
		basename:      basename,
		sizeInBackend: size,
		sizeInMemory:  size,
		eTag:          eTag,
		mode:          uint32(syscall.S_IFREG | parentInode.backend.filePerm),
		mTime:         mTime,
		xTime:         time.Time{},
		// listElement: filled in below
		fhMap:             make(map[uint64]*fhStruct),
		physChildInodeMap: nil,
		virtChildInodeMap: nil,
		cache:             make(map[uint64]*cacheLineStruct),
	}

	if parentInode.objectPath == "" {
		fileObjectInode.objectPath = basename
	} else {
		fileObjectInode.objectPath = parentInode.objectPath + basename
	}

	if isVirt {
		ok = parentInode.virtChildInodeMap.Put(basename, fileObjectInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.Put(basename, fileObjectInode.inodeNumber) returned !ok")
		}
	} else {
		ok = parentInode.physChildInodeMap.Put(basename, fileObjectInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.Put(basename, fileObjectInode.inodeNumber) returned !ok")
		}
	}

	globals.inodeMap[fileObjectInode.inodeNumber] = fileObjectInode

	fileObjectInode.touch(nil)

	return
}

// `touch` is called to ensure an inode that should be on globals.inodeEvictionLRU has the
// appropriate .xTime. `touch` will optionally update .mTime as well. If the inode should
// not be on globals.inodeEvictionLRU, its .listElement will be nil.
func (inode *inodeStruct) touch(mTimeAsInterface interface{}) {
	var (
		ok bool
	)

	if mTimeAsInterface != nil {
		inode.mTime, ok = mTimeAsInterface.(time.Time)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] mTimeAsInterface.(time.Time) returned !ok")
		}
	}

	if inode.listElement != nil {
		globals.inodeEvictionLRU.Remove(inode.xTime, inode.listElement)
		inode.xTime = time.Time{}
		inode.listElement = nil
	}

	switch inode.inodeType {
	case FileObject:
		if len(inode.fhMap) == 0 {
			if inode.isVirt {
				inode.xTime = time.Now().Add(globals.config.virtualFileTTL)
			} else {
				inode.xTime = time.Now().Add(globals.config.evictableInodeTTL)
			}
			inode.listElement = globals.inodeEvictionLRU.Put(inode.xTime, inode.inodeNumber)
		}
	case FUSERootDir:
		// Never placed on any of globals.inodeEvictionLRU
	case BackendRootDir:
		// Never placed on any of globals.inodeEvictionLRU
	case PseudoDir:
		if (len(inode.fhMap) == 0) && (inode.physChildInodeMap.Len() == 0) && (inode.virtChildInodeMap.Len() == 2) {
			if inode.isVirt {
				inode.xTime = time.Now().Add(globals.config.virtualDirTTL)
			} else {
				inode.xTime = time.Now().Add(globals.config.evictableInodeTTL)
			}
			inode.listElement = globals.inodeEvictionLRU.Put(inode.xTime, inode.inodeNumber)
		}
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] inode.inodeType(%v) must be one of FileObject(%v), FUSERootDir(%v), BackendRootDir(%v), or PseudoDir(%v)", inode.inodeType, FileObject, FUSERootDir, BackendRootDir, PseudoDir)
	}
}

// `inodeEvictor` is a goroutine that periodically monitors the globals.inodeEvictionLRU
// to see if any "phys" inodes should be evicted or "virt" inodes should be expired.
func inodeEvictor() {
	var (
		childInode       *inodeStruct
		childInodeNumber uint64
		listElement      *list.Element
		ok               bool
		parentInode      *inodeStruct
		ticker           *time.Ticker
		timeNow          time.Time
		xTime            time.Time
	)

	ticker = time.NewTicker(globals.config.evictableInodeTTL)

	for {
		select {
		case <-ticker.C:
			globals.Lock()

			// Scan globals.inodeEvictionLRU looking for expired inodes to evict

			timeNow = time.Now()

			for {
				xTime, listElement, childInodeNumber, ok = globals.inodeEvictionLRU.Front()
				if !ok || (xTime.After(timeNow)) {
					break
				}

				globals.inodeEvictionLRU.Remove(xTime, listElement)

				childInode, ok = globals.inodeMap[childInodeNumber]
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok")
				}

				parentInode, ok = globals.inodeMap[childInode.parentInodeNumber]
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeMap[childInode.parentInodeNumber] returned !ok")
				}

				if childInode.isVirt {
					ok = parentInode.virtChildInodeMap.DeleteByKey(childInode.basename)
					if !ok {
						dumpStack()
						globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInode.basename) returned !ok")
					}
				} else {
					ok = parentInode.physChildInodeMap.DeleteByKey(childInode.basename)
					if !ok {
						dumpStack()
						globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInode.basename) returned !ok")
					}
				}

				delete(globals.inodeMap, childInodeNumber)

				parentInode.touch(nil)
			}

			globals.Unlock()
		case <-globals.inodeEvictorContext.Done():
			ticker.Stop()
			return
		}
	}
}

// `findChildInode` is called to locate or create a child's inodeStruct. The return `ok` indicates
// that either the child's inodeStruct was already known or has been created in the cases where
// an existing object or object prefix is found. Callers should already hold globals.Lock().
func (parentInode *inodeStruct) findChildInode(basename string) (childInode *inodeStruct, ok bool) {
	var (
		childInodeNumber   uint64
		dirOrFilePath      string
		err                error
		statDirectoryInput *statDirectoryInputStruct
		statFileInput      *statFileInputStruct
		statFileOutput     *statFileOutputStruct
	)

	defer func() {
		parentInode.touch(nil)

		if ok {
			childInode.touch(nil)
		}
	}()

	// First see if we already know about the childInode

	childInodeNumber, ok = parentInode.physChildInodeMap.GetByKey(basename)
	if ok {
		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [findChildInode() case 1]")
		}

		// [TODO] We might want to (1) validate the object or prefix exists and (2) if it doesn't and this is a PseudoDir, convert it & all descendents to "virt"

		return
	}

	childInodeNumber, ok = parentInode.virtChildInodeMap.GetByKey(basename)
	if ok {
		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [findChildInode() case 2]")
		}

		// [TODO] We might want to (1) validate the object or prefix doesn't exist and (2) if it does and this is a PseudoDir, convert it to "phys"

		return
	}

	// We didn't already know about the childInode

	if parentInode.objectPath == "" {
		dirOrFilePath = basename
	} else {
		dirOrFilePath = parentInode.objectPath + basename
	}

	// Let's look for an existing object in the backend

	statFileInput = &statFileInputStruct{
		filePath: dirOrFilePath,
		ifMatch:  "",
	}

	statFileOutput, err = statFileWrapper(parentInode.backend.context, statFileInput)
	if err == nil {
		// We found an existing object in the backend, so let's create a FileObject inode for it

		childInode = parentInode.createFileObjectInode(false, basename, statFileOutput.size, statFileOutput.eTag, statFileOutput.mTime)

		ok = true
		return
	}

	// No object found in the backend... what about an object prefix?
	// Note: By convention, we must modify dirOrFileOPath to end in "/"

	dirOrFilePath += "/"

	statDirectoryInput = &statDirectoryInputStruct{
		dirPath: dirOrFilePath,
	}

	_, err = statDirectoryWrapper(parentInode.backend.context, statDirectoryInput)
	if err == nil {
		// We found an existing object prefix in the backend, ao let's create a PseudoDir inode for it

		childInode = parentInode.createPseudoDirInode(false, basename)

		ok = true
		return
	}

	// We found neither an object nor an object prefix in the backend... so we fail

	childInode = nil
	ok = false

	return
}

// `findChildDirInode` is called to locate, or create if missing, a child directory inodeStruct.
func (parentInode *inodeStruct) findChildDirInode(basename string) (childDirInode *inodeStruct) {
	var (
		childDirInodeNumber uint64
		ok                  bool
	)

	defer func() {
		parentInode.touch(nil)
		childDirInode.touch(nil)
	}()

	// First see if we already know about the childInode

	childDirInodeNumber, ok = parentInode.physChildInodeMap.GetByKey(basename)
	if ok {
		childDirInode, ok = globals.inodeMap[childDirInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childDirInodeNumber] returned !ok [findChildDirInode() case 1]")
		}

		// [TODO] We might want to validate that childDirInode.inodeType == PseudoDir
		// [TODO] We might want to (1) validate the prefix exists and (2) if it doesn't, convert it & all descendents to "virt"

		return
	}

	childDirInodeNumber, ok = parentInode.virtChildInodeMap.GetByKey(basename)
	if ok {
		childDirInode, ok = globals.inodeMap[childDirInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childDirInodeNumber] returned !ok [findChildDirInode() case 1]")
		}

		// [TODO] We might want to validate that childDirInode.inodeType == PseudoDir
		// [TODO] We might want to (1) validate the prefix doesn't exist and (2) if it does, convert it to "phys"

		return
	}

	// We didn't already know about the childInode... so just create it

	childDirInode = parentInode.createPseudoDirInode(false, basename)

	return
}

// `findChildFileInode` is called to locate, or create if missing, a child file inodeStruct.
func (parentInode *inodeStruct) findChildFileInode(basename, eTag string, mTime time.Time, size uint64) (childFileInode *inodeStruct) {
	var (
		childFileInodeNumber uint64
		ok                   bool
	)

	defer func() {
		parentInode.touch(nil)
		childFileInode.touch(nil)
	}()

	// First see if we already know about the childInode

	childFileInodeNumber, ok = parentInode.physChildInodeMap.GetByKey(basename)
	if ok {
		childFileInode, ok = globals.inodeMap[childFileInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childFileInodeNumber] returned !ok [findChildFileInode() case 1]")
		}

		// [TODO] We might want to validate that childFileInode.inodeType == FileObject
		// [TODO] We might want to (1) validate the object exists and (2) if it doesn't, convert it to "virt"

		return
	}

	childFileInodeNumber, ok = parentInode.virtChildInodeMap.GetByKey(basename)
	if ok {
		childFileInode, ok = globals.inodeMap[childFileInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childFileInodeNumber] returned !ok [findChildFileInode() case 1]")
		}

		// [TODO] We might want to validate that childFileInode.inodeType == FileObject
		// [TODO] We might want to (1) validate the object doesn't exist and (2) if it does, convert it to "phys"

		return
	}

	// We didn't already know about the childFileInode... so just create it

	childFileInode = parentInode.createFileObjectInode(false, basename, size, eTag, mTime)

	return
}

const (
	DUMP_FS_DIR_INDENT = "    "
)

// `dumpFS` logs the entire file system. It must be called without holding globals.Lock().
func dumpFS() {
	var (
		ok           bool
		rootDirInode *inodeStruct
	)

	globals.Lock()

	rootDirInode, ok = globals.inodeMap[FUSERootDirInodeNumber]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap[FUSERootDirInodeNumber] returned !ok")
	}
	if rootDirInode.inodeType != FUSERootDir {
		dumpStack()
		globals.logger.Fatalf("[FATAL] rootDirInode.inodeType(%v) should have been FUSERootDir(%v)", rootDirInode.inodeType, FUSERootDir)
	}

	rootDirInode.dumpFS("", FUSERootDirInodeNumber, "")

	globals.Unlock()
}

// `dumpFS` called on a particular inode recursively dumps a file system element.
func (thisInode *inodeStruct) dumpFS(indent string, expectedInodeNumber uint64, expectedBasename string) {
	var (
		childInode         *inodeStruct
		childInodeBasename string
		childInodeMapIndex int
		childInodeMapLen   int
		childInodeNumber   uint64
		nextIndent         = indent + DUMP_FS_DIR_INDENT
		ok                 bool
		thisInodeBasename  string
	)

	if thisInode.inodeNumber != expectedInodeNumber {
		dumpStack()
		globals.logger.Fatalf("[FATAL] thisInode.inodeNumber(%v) != expectedInodeNumber(%v)", thisInode.inodeNumber, expectedInodeNumber)
	}
	if thisInode.basename != expectedBasename {
		dumpStack()
		globals.logger.Fatalf("[FATAL] thisInode.basename(\"%s\") != expectedBasename(\"%s\")", thisInode.basename, expectedBasename)
	}

	switch thisInode.inodeType {
	case FileObject:
		thisInodeBasename = thisInode.basename
	case FUSERootDir:
		thisInodeBasename = "[FUSERootDir]"
	case BackendRootDir:
		thisInodeBasename = "[BackendRootDir] \"" + thisInode.basename + "\"(" + thisInode.objectPath + ")"
	case PseudoDir:
		thisInodeBasename = "[PseudoDir]      \"" + thisInode.basename + "\"(" + thisInode.objectPath + ")"
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] dirInode.inodeType should == FUSERootDir(%v) or BackendRootDir(%v) or PseudoDir(%v), not FileObject(%v) nor what was found: %v", FUSERootDir, BackendRootDir, PseudoDir, FileObject, thisInode.inodeType)
	}

	globals.logger.Printf("[INDO] %s%5d \"%s\"", indent, thisInode.inodeNumber, thisInodeBasename)

	if thisInode.inodeType == FileObject {
		return
	}

	childInodeMapLen = thisInode.virtChildInodeMap.Len()

	for childInodeMapIndex = range childInodeMapLen {
		childInodeBasename, childInodeNumber, ok = thisInode.virtChildInodeMap.GetByIndex(childInodeMapIndex)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] thisInode.virtChildInodeMap.GetByIndex(childInodeMapIndex) returned !ok")
		}

		if childInodeBasename == DotDirEntryBasename {
			if childInodeNumber != thisInode.inodeNumber {
				dumpStack()
				globals.logger.Fatalf("[FATAL] childInodeNumber(%v) != thisInode.inodeNumber(%v) [case virt]", childInodeNumber, thisInode.inodeNumber)
			}
			globals.logger.Printf("[INFO] %s%5d \"%s\"", nextIndent, childInodeNumber, childInodeBasename)
			continue
		}

		if childInodeBasename == DotDotDirEntryBasename {
			if childInodeNumber != thisInode.parentInodeNumber {
				dumpStack()
				globals.logger.Fatalf("[FATAL] childInodeNumber(%v) != thisInode.parentInodeNumber(%v) [case virt]", childInodeNumber, thisInode.parentInodeNumber)
			}
			globals.logger.Printf("[INFO] %s%5d \"%s\"", nextIndent, childInodeNumber, childInodeBasename)
			continue
		}

		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [case virt]")
		}

		childInode.dumpFS(nextIndent, childInodeNumber, childInodeBasename)
	}

	childInodeMapLen = thisInode.physChildInodeMap.Len()

	for childInodeMapIndex = range childInodeMapLen {
		childInodeBasename, childInodeNumber, ok = thisInode.physChildInodeMap.GetByIndex(childInodeMapIndex)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] thisInode.physChildInodeMap.GetByIndex(childInodeMapIndex) returned !ok")
		}

		if childInodeBasename == DotDirEntryBasename {
			if childInodeNumber != thisInode.inodeNumber {
				dumpStack()
				globals.logger.Fatalf("[FATAL] childInodeNumber(%v) != thisInode.inodeNumber(%v) [case phys]", childInodeNumber, thisInode.inodeNumber)
			}
			globals.logger.Printf("[INFO] %s%5d \"%s\"", nextIndent, childInodeNumber, childInodeBasename)
			continue
		}

		if childInodeBasename == DotDotDirEntryBasename {
			if childInodeNumber != thisInode.parentInodeNumber {
				dumpStack()
				globals.logger.Fatalf("[FATAL] childInodeNumber(%v) != thisInode.parentInodeNumber(%v) [case phys]", childInodeNumber, thisInode.parentInodeNumber)
			}
			globals.logger.Printf("[INFO] %s%5d \"%s\"", nextIndent, childInodeNumber, childInodeBasename)
			continue
		}

		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [case phys]")
		}

		childInode.dumpFS(nextIndent, childInodeNumber, childInodeBasename)
	}
}
