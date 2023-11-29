from io import BytesIO
from typing import BinaryIO, Dict

import websockets


async def chat_socket(
        file_path,
        websocket_uri: str,
        headers: Dict = None,
        max_size: int = 25000000,
        ping_interval: int = 60,
        ping_timeout: int = None,
    ) -> BinaryIO:
    try: 
        async with websockets.connect(
            websocket_uri, 
            max_size=max_size,
            ping_timeout=ping_timeout, 
            ping_interval=ping_interval,
            extra_headers=headers,
            ) as websocket:
            
            print("Sending dataframe to server") 
            with open(file_path, "rb") as file:
                file_content = file.read()
                await websocket.send(file_content)
            response = await websocket.recv()
            print(response)


            print("You can now chat with your data. Write 'exit' to end chat.")
            while True:
                msg = input("Prompt:")
                await websocket.send(msg)
                
                response = await websocket.recv()

                if msg == "exit":
                    break
                else: # Otherwise it will print the dataset
                    print(f"Response:\n {response}")

            return BytesIO(response)
            # return await websocket.recv()

    except websockets.exceptions.ConnectionClosedError as e:
        print(f"Connection to the server closed unexpectedly: {e}")
    except websockets.exceptions.InvalidURI as e:
        print(f"Invalid URI provided: {e}")
    except websockets.exceptions.WebSocketException as e:
        print(f"WebSocket exception occurred: {e}")
    except FileNotFoundError as e:
        print(f"File not found error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")