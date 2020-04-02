import asyncio
import json
import os

auth_lock = asyncio.Lock()
user_data_lock = asyncio.Lock()


class EpicHolder(object):
    def __init__(self):
        if os.path.isfile("data"):
            with open("data") as fin:
                self.data = json.load(fin)
                return
        self.data = {}

    async def get(self, user, key):
        key = f"{user}/{key}"
        if key in self.data:
            return self.data[key]
        return None

    async def put(self, user, key, value):
        key = f"{user}/{key}"
        async with user_data_lock:
            self.data[key] = value
            with open("data", "w") as fout:
                json.dump(self.data, fout)


eh = EpicHolder()


class Talker(object):
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.auth_user_data = None

    async def say(self, message: str):
        if message[-1] != '\n':
            message += '\n'
        self.writer.write(message.encode())
        await self.writer.drain()

    async def wrong_command(self, msg=None):
        data = {"status": "error", "message": msg or "unexpected message"}
        return await self.say(json.dumps(data))

    async def ok_command(self, msg=None):
        data = {"status": "OK"}
        if msg:
            data["message"] = msg

        return await self.say(json.dumps(data))

    async def get_command(self):
        if self.reader.at_eof():
            return {"method_name": "EMPTY"}
        line = await self.reader.readline()
        line = line.strip()

        try:
            if not line.startswith(b"{"):
                raise Exception("ad-hoc for python json lib")
            command_data = json.loads(line)
        except:
            await self.wrong_command("Can't parse this json data. Please, try again")
            return {"method_name": "EMPTY"}

        if "method_name" not in command_data or command_data["method_name"] == "EMPTY":
            await self.wrong_command("Can't find valid field method_name in your response. Please, try again")
            return {"method_name": "EMPTY"}

        return command_data

    async def start_handle(self):
        while not self.reader.at_eof():
            command_data = await self.get_command()
            if command_data["method_name"] == "AUTH":
                await self.auth(command_data)

            elif command_data["method_name"] == "SEND":
                await self.send_data(command_data)

            elif command_data["method_name"] == "GET":
                await self.get_data(command_data)

            elif command_data["method_name"] == "REG":
                await self.reg(command_data)

            elif command_data["method_name"] == "EMPTY":
                pass

            else:
                await self.wrong_command("Unknown method name")

        await self.say("Good bye")

    @staticmethod
    def get_auth_data_array():
        try:
            fd = open("auth_data")
        except:
            return {}

        try:
            data = json.load(fd)
        except:
            fd.close()
            return {}

        fd.close()
        return data

    async def reg(self, command_data):
        if "user" not in command_data or "pass" not in command_data:
            return await self.wrong_command("Undefined pass or user field")

        async with auth_lock:
            auth_data = Talker.get_auth_data_array()
            if command_data["user"] in auth_data:
                return await self.wrong_command("User with this name already exists")

            auth_data[command_data["user"]] = command_data["pass"]
            with open("auth_data", "w") as fout:
                json.dump(auth_data, fout)

        return await self.ok_command("Welcome!")

    async def auth(self, command_data):
        if "user" not in command_data or "pass" not in command_data:
            return await self.wrong_command("Undefined pass or user field")

        async with auth_lock:
            auth_data = Talker.get_auth_data_array()

            if command_data["user"] in auth_data and auth_data[command_data["user"]] == command_data["pass"]:
                self.auth_user_data = command_data["user"]
                return await self.ok_command("Here we go again")

            return await self.wrong_command("Wrong auth")

    async def send_data(self, command_data):
        if not self.auth_user_data:
            return await self.wrong_command("We don't know who you are")

        if "label" not in command_data or "point" not in command_data:
            return await self.wrong_command("Undefined label or point field")

        if not isinstance(command_data["point"], int):
            return await self.wrong_command("Wrong integer in point")
        cur_data = await (eh.get(self.auth_user_data, command_data["label"])) or 0
        cur_data += command_data["point"]
        await eh.put(self.auth_user_data, command_data["label"], cur_data)
        return await self.ok_command("Accepted")

    async def get_data(self, command_data):
        if not self.auth_user_data:
            return await self.wrong_command("We don't know who you are")

        if "label" not in command_data:
            return await self.wrong_command("Undefined label")

        data = await eh.get(self.auth_user_data, command_data["label"])

        return await self.say(json.dumps({"status": "OK", "data": data}))


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    talker = Talker(reader, writer)
    return await talker.start_handle()


def run(host, port):
    loop = asyncio.get_event_loop()

    server = asyncio.start_server(handle, host, port, loop=loop)
    server = loop.run_until_complete(server)

    # Serve requests until Ctrl+C is pressed
    print("Serving on {}".format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    run("0.0.0.0", 8888)
