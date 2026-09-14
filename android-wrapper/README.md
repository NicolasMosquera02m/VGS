# VGS Mobile

This Android wrapper opens the Streamlit application inside a WebView.

## Configure the app URL

Edit `app/src/main/java/com/vgs/mobile/MainActivity.java` and replace:

```java
https://REEMPLAZA-CON-TU-URL-PUBLICA
```

with the public HTTPS URL where the Streamlit app is deployed.

A `localhost` or private container address will not work on a phone.

## Build the APK

Open the `android-wrapper` folder with Android Studio, let it install the Android SDK, then choose:

`Build > Build App Bundle(s) / APK(s) > Build APK(s)`

The generated debug APK is usually placed at:

`app/build/outputs/apk/debug/app-debug.apk`
