Test Procedures
1. Copy the WeaverDataImport table and the WeaverProgramStatus tables into the NoiseMit.gdb in the test/fixtures directory
2. Copy the NoiseBuilding and the SSACARBuilding feature classes into the TestTables.gdb in the test/fixtures directory
3. Move these to a local machine with development tools
4. Load the test fixtures into an enterprise geodatabase
5. Register the data as versioned
6. Copy the connection file from ArcCatalog to the './DBConnections' folder
6. Update all of the parameters with the correct paths to the sde connection files in the DBConnections folder