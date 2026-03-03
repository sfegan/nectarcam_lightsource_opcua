# OPCUA server and test suite for NectarCam calibration light sources

This repository implements a simple (and probably not very robust) reimplementation of the OPC UA server for the NectarCAM calibration liught sources, based on the [MOS version written by Patrick Sizun](https://gitlab.cta-observatory.org/cta-array-elements/mst/nectarcam/software/cal/calibration-box-mos/-/tree/main?ref_type=heads).

It contains three elements
1. `nc_lightsource_opcua_server.py`: the OPCUA server which acts as a bridge between the socket based protocol used by the light sources and the OPC UA network
2. `nc_lightsource_opcua_test_cli.py`: a simple command line interface to call the OPC UA methods and read/monitor the variables
3. `nc_lightsource_emulator.py`: a simple emulator for the light source that responds to commands over the TCP port (and from the terminal).

The three components can be used together to test the server. In three terminal windows run :

### Terminal 1: run the light source emulator

`# python3 nc_lightsource_emulator.py --password HELLO --log-level=ERROR --product-code 0x15`

### Terminal 2: run the OPC UA server

`# python3 nc_lightsource_opcua_server.py --opcua-endpoint=opc.tcp://localhost:4840/nectarcam/ --address=localhost --passwd HELLO --product-code 0x15`

### Terminal 3: run the OPC UA test CLI

`# python3 nc_lightsource_opcua_test_cli.py --endpoint opc.tcp://localhost:4840/nectarcam/`




