# Compilação

Removi os testes de malloc/realloc do configure.ac

```bash
. ../../head-3.x-buildroot/scripts/setup-toolchain.sh --toolchain=armeabi
./configure --host arm-buildroot-linux-gnueabi  --build i686-pc-linux-gnu
make clean
make
```

Ou

```bash
PATH=/usr/local/toolchains/arm-buildroot-linux-gnueabi_sdk-buildroot-kh_4.9-gcc_9-arm/bin:$PATH
./configure --host arm-buildroot-linux-gnueabi  --build i686-pc-linux-gnu
make clean
make
```
