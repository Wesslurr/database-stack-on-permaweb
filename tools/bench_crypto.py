import time
from cryptography.hazmat.primitives.asymmetric import rsa, padding, ed25519
from cryptography.hazmat.primitives import hashes

def main():
    print("Generating keys...")
    rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    ed_key = ed25519.Ed25519PrivateKey.generate()
    data = b"benchmark message for cryptographic signature profiling"

    rsa_times = []
    for _ in range(10):
        t0 = time.perf_counter()
        _ = rsa_key.sign(data, padding.PSS(mgf=padding.MGF1(hashes.SHA512()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA512())
        rsa_times.append((time.perf_counter() - t0) * 1000)

    ed_times = []
    for _ in range(10):
        t0 = time.perf_counter()
        _ = ed_key.sign(data)
        ed_times.append((time.perf_counter() - t0) * 1000)

    print(f"RSA-4096 Sign Avg: {sum(rsa_times)/len(rsa_times):.2f} ms (Min: {min(rsa_times):.2f} ms, Max: {max(rsa_times):.2f} ms)")
    print(f"Ed25519  Sign Avg: {sum(ed_times)/len(ed_times):.4f} ms (Min: {min(ed_times):.4f} ms, Max: {max(ed_times):.4f} ms)")
    print(f"Ratio: RSA-4096 is { (sum(rsa_times)/len(rsa_times)) / (sum(ed_times)/len(ed_times)):.1f}x slower than Ed25519!")

if __name__ == "__main__":
    main()
