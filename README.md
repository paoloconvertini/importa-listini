# importa-listini
## Per il deploy:

1. **Lanciare il comando**:
   * pyinstaller --onefile .\processa_listini.py
2. Copiare il file application.yaml, **se e solo se è stato modificato**, nella cartella dist.
3. Comprimere la cartella dist in zip
4. Aprire **NSIS** e creare il file exe partendo dal file zip del punto 3