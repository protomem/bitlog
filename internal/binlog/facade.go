package binlog

type Facade struct {
	actualKeys  *KeyDir
	mainJournal *KeyValueJournal
}

func NewFacade(driver Driver) *Facade {
	return &Facade{
		actualKeys:  NewKeyDir(),
		mainJournal: NewKeyValueJournal(driver),
	}
}

func (f *Facade) Get(key []byte) ([]byte, bool, error) {
	index, exists := f.actualKeys.Lookup(key)
	if !exists || index == nil {
		return nil, false, nil
	}

	log, err := f.mainJournal.Read(index.LogID)
	if err != nil {
		return nil, false, err
	}

	return log.Value, true, nil
}

func (f *Facade) Set(key, value []byte) error {
	log := NowKeyValueLog(key, value)
	lid, err := f.mainJournal.Write(log)
	if err != nil {
		return err
	}

	index := NewIndex(key, lid)
	f.actualKeys.Insert(index)

	return nil
}

func (f *Facade) Delete(key []byte) error {
	f.actualKeys.Remove(key)

	log := TombstoneKeyValueLog(key)
	lid, err := f.mainJournal.Write(log)
	if err != nil {
		return err
	}

	index := NewIndex(key, lid)
	f.actualKeys.Insert(index)

	return nil
}
