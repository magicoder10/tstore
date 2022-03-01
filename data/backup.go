package data

//
//var databaseDirPathFmt = filepath.Join(storage.DataDir, "databases", "%s")
//var commitsFilePathFmt = filepath.Join("%s", "commits.json")
//var schemasDirPathFmt = filepath.Join("%s", "schemas")
//var schemaFilePathFmt = filepath.Join("%s", "%v.json")
//var entitiesDirPathFmt = filepath.Join("%s", "entities")
//var entityFilePathFmt = filepath.Join("%s", "%v.json")
//
//// TODO: merge & partition into a multiple files to avoid exceeding INode limit
//
//type Storage struct {
//	latestData      Data
//	commitsFilePath string
//	schemasDirPath  string
//	entitiesDirPath string
//}
//
//func (s Storage) ReadAllCommits() ([]Commit, error) {
//	// TODO: cache in & read from memory
//
//	buf, err := ioutil.ReadFile(s.commitsFilePath)
//	if err != nil {
//		return []Commit{}, err
//	}
//
//	var commits []Commit
//	err = json.Unmarshal(buf, &commits)
//	return commits, err
//}
//
//func (s Storage) WriteAllCommits(commits []Commit) error {
//	buf, err := json.MarshalIndent(commits, "", storage.JSONIndent)
//	if err != nil {
//		return err
//	}
//
//	return ioutil.WriteFile(s.commitsFilePath, buf, storage.DefaultFileMode)
//}
//
//func (s Storage) ReadAllEntities(commitId uint64) map[uint64]Entity {
//	return s.latestData.EntityHistories.ListLatestValuesAt(commitId)
//}
//
//func (s Storage) ReadEntity(commitId uint64, entityID uint64) (Entity, bool) {
//	return s.latestData.EntityHistories.FindLatestValueAt(commitId, entityID)
//}
//
//func (s Storage) WriteEntity(commitId uint64, entity Entity) error {
//	buf, err := json.MarshalIndent(entity, "", storage.JSONIndent)
//	if err != nil {
//		return err
//	}
//
//	err = ioutil.WriteFile(fmt.Sprintf(entityFilePathFmt, s.entitiesDirPath, entity.ID), buf, storage.DefaultFileMode)
//	if err != nil {
//		return err
//	}
//
//	s.latestData.entities[entity.ID] = entity
//	return nil
//}
//
//func (s Storage) DeleteEntity(entityID uint64) error {
//	err := os.Remove(fmt.Sprintf(entityFilePathFmt, s.entitiesDirPath, entityID))
//	if err != nil {
//		return err
//	}
//
//	delete(s.latestData.entities, entityID)
//	return nil
//}
//
//func (s Storage) ReadAllSchemas(commitID uint64) map[string]Schema {
//	return s.latestData.SchemaHistories.ListLatestValuesAt(commitID)
//}
//
//func (s Storage) ReadSchema(commitID uint64, schemaName string) (Schema, bool) {
//	return s.latestData.SchemaHistories.FindLatestValueAt(commitID, schemaName)
//}
//
//func (s Storage) WriteSchema(schema Schema) error {
//	buf, err := json.MarshalIndent(schema, "", storage.JSONIndent)
//	if err != nil {
//		return err
//	}
//
//	err = ioutil.WriteFile(fmt.Sprintf(schemaFilePathFmt, s.schemasDirPath, schema.Name), buf, storage.DefaultFileMode)
//	if err != nil {
//		return err
//	}
//
//	s.latestData.schemas[schema.Name] = schema
//	return nil
//}
//
//func (s Storage) DeleteSchema(schemaName string) error {
//	err := os.Remove(fmt.Sprintf(schemaFilePathFmt, s.schemasDirPath, schemaName))
//	if err != nil {
//		return err
//	}
//
//	delete(s.latestData.schemas, schemaName)
//	return nil
//}
//
//func (s *Storage) feedLatestData() {
//	schemas, err := s.ReadAllSchemas()
//	if err != nil {
//		schemas = make([]Schema, 0)
//	}
//
//	entities, err := s.ReadAllEntities()
//	if err != nil {
//		entities = make([]Entity, 0)
//	}
//
//	indexedSchemas := history.NewKeyValue[uint64, string, Schema, map[string]Type](
//		func() history.ValueHistory[string, Schema, map[string]Type] {
//			NewSchemaValueHistory()
//		})
//	for _, schema := range schemas {
//		indexedSchemas[schema.Name] = schema
//	}
//
//	indexedEntities := make(map[uint64]Entity)
//	for _, entity := range entities {
//		indexedEntities[entity.ID] = entity
//	}
//
//	s.latestData = Data{
//		SchemaHistories:  indexedSchemas,
//		EntityHistories: indexedEntities,
//	}
//}
//
//func NewStorage(dbName string) (Storage, error) {
//	databaseDirPath := fmt.Sprintf(databaseDirPathFmt, dbName)
//	schemasDirPath := fmt.Sprintf(schemasDirPathFmt, databaseDirPath)
//	entitiesDirPath := fmt.Sprintf(entitiesDirPathFmt, databaseDirPath)
//
//	err := os.MkdirAll(databaseDirPath, storage.DefaultFileMode)
//	if err != nil {
//		return Storage{}, err
//	}
//
//	err = os.MkdirAll(schemasDirPath, storage.DefaultFileMode)
//	if err != nil {
//		return Storage{}, err
//	}
//
//	err = os.MkdirAll(entitiesDirPath, storage.DefaultFileMode)
//	if err != nil {
//		return Storage{}, err
//	}
//
//	st := Storage{
//		schemasDirPath:  schemasDirPath,
//		entitiesDirPath: entitiesDirPath,
//		commitsFilePath: fmt.Sprintf(commitsFilePathFmt, databaseDirPath),
//	}
//
//	st.feedLatestData()
//
//	return st, nil
//}
