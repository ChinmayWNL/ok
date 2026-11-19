import json
import time
import urllib.request
import urllib.error

REG = "http://localhost:8081"
SUBJECT = "patient-data-value"
CT = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        REG + path, data=data, method=method, headers=CT
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or "null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or "null")


def schema_body(path):
    with open(path) as f:
        return {"schema": json.dumps(json.load(f))}


def step(n, title):
    print(f"\n{'='*70}\nSTEP {n}: {title}\n{'='*70}")


step(1, "Wait for Schema Registry")
for _ in range(30):
    try:
        print("Registry is up. Existing subjects:", call("GET", "/subjects")[1])
        break
    except Exception:
        time.sleep(2)
else:
    raise RuntimeError("Schema Registry did not become available")

call("DELETE", f"/subjects/{SUBJECT}")
call("DELETE", f"/subjects/{SUBJECT}?permanent=true")

step(2, "Register V1 (patient_id, patient_name, age, heart_rate)")
print(call("POST", f"/subjects/{SUBJECT}/versions",
           schema_body("schemas/patient-v1.avsc")))

step(3, "Set compatibility of subject to BACKWARD")
print(call("PUT", f"/config/{SUBJECT}",
           {"compatibility": "BACKWARD"}))
print("Current config:", call("GET", f"/config/{SUBJECT}")[1])

step(4, "Check V2 (adds blood_pressure WITH default) for compatibility")
print(call("POST", f"/compatibility/subjects/{SUBJECT}/versions/latest",
           schema_body("schemas/patient-v2.avsc")))

step(5, "Register V2")
print(call("POST", f"/subjects/{SUBJECT}/versions",
           schema_body("schemas/patient-v2.avsc")))
print("-> V2 ACCEPTED")

step(6, "Check incompatible V3 (age int->string, new required field w/o default)")
v3 = {
    "type": "record",
    "name": "Patient",
    "namespace": "com.hospital",
    "fields": [
        {"name": "patient_id", "type": "int"},
        {"name": "patient_name", "type": "string"},
        {"name": "age", "type": "string"},
        {"name": "heart_rate", "type": "int"},
        {"name": "blood_pressure", "type": "string", "default": "Unknown"},
        {"name": "diagnosis", "type": "string"}
    ]
}
print(call("POST", f"/compatibility/subjects/{SUBJECT}/versions/latest",
           {"schema": json.dumps(v3)}))