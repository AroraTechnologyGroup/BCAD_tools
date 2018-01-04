Test Procedures
1. Copy the WeaverDataImport table and the WeaverProgramStatus tables into the NoiseMit.gdb in the test/fixtures directory
2. Copy the NoiseBuilding and the SSACARBuilding feature classes into the TestTables.gdb in the test/fixtures directory
3. Copy the Lease.dbo table and the Lease.bcad table into the masterGDB for testing
4. Move these to a local machine with development tools
5. Load the test fixtures into an enterprise geodatabase
6. Register the data as versioned !!
7. Copy the connection file from ArcCatalog to the './DBConnections' folder
8. Update all of the parameters with the correct paths to the sde connection files in the DBConnections folder
9. To test..... 

After copying the tables and features classes run the Test Suite.  

    Result - No changes should be updated except for the last scanned date.

Remove all of the rows in each of the Geodatabaes Tables, run the Test Suite.

    Result - All of the rows should be added to the GDB Table.

Delete half of the rows from the GDB table, and half from the Source Table run the Test Suite.
    
    Result - Some rows should be added and some removed
    
Clear the attributes being updated on the buildings feature class, run the Test Suite.

    Result - The buildings should be updated with values
