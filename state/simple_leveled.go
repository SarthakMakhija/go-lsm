package state

type SimpleLeveledCompactionOptions struct {
	sizeRatioPercentage          uint
	maxLevels                    uint
	level0FilesCompactionTrigger uint
}

type SimpleLeveledCompactionTask struct {
	upperLevel           int
	lowerLevel           int
	upperLevelSSTableIds []uint64
	lowerLevelSSTableIds []uint64
}

type SimpleLeveledCompactionController struct {
	options SimpleLeveledCompactionOptions
}

func NewSimpleLeveledCompactionController(options SimpleLeveledCompactionOptions) SimpleLeveledCompactionController {
	return SimpleLeveledCompactionController{
		options: options,
	}
}

// GenerateCompactionTask TODO: Concurrency
func (controller SimpleLeveledCompactionController) GenerateCompactionTask(state *StorageState) SimpleLeveledCompactionTask {
	var levelSizes []int
	levelSizes = append(levelSizes, len(state.l0SSTableIds))

	for _, level := range state.levels {
		levelSizes = append(levelSizes, len(level.ssTableIds))
	}
	for level := 0; level < int(controller.options.maxLevels); level++ {
		if level == 0 {
			if levelSizes[level] < int(controller.options.level0FilesCompactionTrigger) {
				continue
			}
		}
		lowerLevel := level + 1
		sizeRatioPercentage := (float64(levelSizes[lowerLevel]) / float64(levelSizes[level])) * 100
		if sizeRatioPercentage < float64(controller.options.sizeRatioPercentage) {
			println("Triggering compaction between levels ", level, lowerLevel)
			var upperLevel int
			if level == 0 {
				upperLevel = -1
			} else {
				upperLevel = level
			}
			return SimpleLeveledCompactionTask{
				upperLevel:           upperLevel,
				lowerLevel:           lowerLevel,
				upperLevelSSTableIds: state.orderedSSTableIds(level),
				lowerLevelSSTableIds: state.orderedSSTableIds(lowerLevel),
			}
		}
	}
	return SimpleLeveledCompactionTask{}
}
