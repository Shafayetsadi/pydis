import time


class Database:
    def __init__(self):
        self.data = {}

    def set(self, key, value, expire=None):
        if expire:
            expire_at = time.time() + int(expire) / 1000
        else:
            expire_at = None
        self.data[key] = {
            "value": value,
            "expire_at": expire_at
        }

    def get(self, key):
        if key in self.data:
            record = self.data[key]
            if record["expire_at"] is None or record["expire_at"] > time.time():
                return record["value"]
            else:
                del self.data[key]
                return None
        return None

    def delete(self, key):
        if key in self.data:
            del self.data[key]
            return True
        return False


if __name__ == "__main__":
    db = Database()
    db.set("Shafayet", "21")
    print(db.get("Shafayet"))
    db.set("Sadi", "22")
    print(db.get("Sadi"))
    print(db.get("Shafayet"))
