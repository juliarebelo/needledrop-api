const express = require('express');
const cors = require('cors');
const app = express();

app.use(cors());
app.use(express.json());

const db = require('./db.json'); 

app.get('/api/usuarios/me', (req, res) => {
  const usuario = db.usuarios.find(u => u.id === "1");
  res.json(usuario);
});

app.get('/api/me/playlists', (req, res) => {
  const playlistsDoUsuario = db.playlists.filter(p => p.usuarioId === "1");
  res.json(playlistsDoUsuario);
});

app.get('/api/recomendacoes', (req, res) => {
  res.json(db.albuns);
});

app.listen(3000, () => console.log('API rodando na porta 3000'));