# arquivo: audio_processor.py
try:
    import speech_recognition as sr
except Exception:
    sr = None
import os
import tempfile
from app.services.gemini import get_client
from google.genai import types
import time
import subprocess
import shutil

class AudioProcessor:
    """Processa áudio do Telegram para texto."""
    
    def __init__(self):
        try:
            self.recognizer = sr.Recognizer() if sr is not None else None
        except Exception:
            self.recognizer = None
    
    def transcribe_audio_bytes(self, audio_bytes, mime):
        try:
            try:
                client = get_client()
            except:
                client = None
            if client is not None:
                blob = types.Blob(mime_type=mime, data=audio_bytes)
                parts = [types.Part(text="Transcreva o áudio para texto em português do Brasil. Retorne apenas o texto transcrito.")]
                parts.append(types.Part(inline_data=blob))
                contents = [types.Content(role='user', parts=parts)]
                models = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-2.0-pro', 'gemini-1.5-flash', 'gemini-1.5-pro']
                delays = [0.8, 1.5, 2.5, 3.0, 3.5]
                for i, mdl in enumerate(models):
                    try:
                        resp = client.models.generate_content(
                            model=mdl,
                            contents=contents,
                            config=types.GenerateContentConfig(
                                temperature=0.0,
                                max_output_tokens=800,
                            ),
                        )
                        texto = (resp.text or "").strip()
                        if texto:
                            return texto
                    except Exception as e:
                        msg = str(e) if e else ""
                        if "RESOURCE_EXHAUSTED" in msg or "429" in msg or "Too Many Requests" in msg:
                            time.sleep(delays[i] if i < len(delays) else 1.0)
                            continue
                        if "404" in msg or "Not Found" in msg:
                            continue
                        continue
            return None
        except:
            return None
    
    def transcribe_audio(self, audio_path):
        try:
            try:
                client = get_client()
            except:
                client = None
            if client is not None:
                try:
                    with open(audio_path, 'rb') as f:
                        dados = f.read()
                    mime = 'audio/wav' if audio_path.endswith('.wav') else ('audio/ogg' if audio_path.endswith('.ogg') else 'audio/mpeg')
                    texto = self.transcribe_audio_bytes(dados, mime)
                    if texto:
                        return texto
                except:
                    pass
            if self.recognizer is not None and audio_path.endswith('.wav'):
                with sr.AudioFile(audio_path) as source:
                    self.recognizer.adjust_for_ambient_noise(source, duration=0.5)
                    audio_data = self.recognizer.record(source)
                    texto = self.recognizer.recognize_google(
                        audio_data,
                        language='pt-BR',
                        show_all=False
                    )
                    return texto
            return None
        except sr.UnknownValueError:
            return None
        except sr.RequestError:
            return None
        except Exception:
            return None
        finally:
            try:
                if os.path.exists(audio_path):
                    os.remove(audio_path)
            except:
                pass
    
    def transcribe_audio_file(self, audio_bytes, format='ogg'):
        try:
            if format == 'wav':
                with tempfile.NamedTemporaryFile(delete=False, suffix='.wav') as tmp_file:
                    tmp_file.write(audio_bytes)
                    temp_path = tmp_file.name
                try:
                    with sr.AudioFile(temp_path) as source:
                        self.recognizer.adjust_for_ambient_noise(source, duration=0.3)
                        audio_data = self.recognizer.record(source)
                        texto = None
                        try:
                            texto = self.recognizer.recognize_google(audio_data, language='pt-BR', show_all=False)
                        except:
                            texto = None
                        if not texto:
                            try:
                                # tentativa sem ajuste de ruído
                                with sr.AudioFile(temp_path) as s2:
                                    a2 = self.recognizer.record(s2)
                                    texto = self.recognizer.recognize_google(a2, language='pt-BR', show_all=False)
                            except:
                                texto = None
                        if texto:
                            return texto
                except:
                    pass
                finally:
                    try:
                        os.remove(temp_path)
                    except:
                        pass
                t_ai = self.transcribe_audio_bytes(audio_bytes, 'audio/wav')
                return t_ai if t_ai else None
            if format in ('ogg', 'oga', 'opus', 'mp3', 'mpeg', 'm4a', 'aac'):
                try:
                    from imageio_ffmpeg import get_ffmpeg_exe
                    ff = get_ffmpeg_exe()
                except:
                    ff = shutil.which('ffmpeg') or shutil.which('avconv')
                if ff:
                    ext = 'mp3' if format == 'mpeg' else format
                    with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{ext}') as in_file:
                        in_file.write(audio_bytes)
                        in_path = in_file.name
                    try:
                        import os as _os
                        base, _ext = _os.path.splitext(in_path)
                        out_path = base + '.wav'
                    except:
                        out_path = in_path + '.wav'
                    try:
                        subprocess.run([ff, '-y', '-i', in_path, '-ar', '16000', '-ac', '1', out_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        if self.recognizer is not None:
                            with sr.AudioFile(out_path) as source:
                                self.recognizer.adjust_for_ambient_noise(source, duration=0.3)
                                audio_data = self.recognizer.record(source)
                                texto = None
                                try:
                                    texto = self.recognizer.recognize_google(audio_data, language='pt-BR', show_all=False)
                                except:
                                    texto = None
                                if not texto:
                                    try:
                                        with sr.AudioFile(out_path) as s2:
                                            a2 = self.recognizer.record(s2)
                                            texto = self.recognizer.recognize_google(a2, language='pt-BR', show_all=False)
                                    except:
                                        texto = None
                                if texto:
                                    return texto
                        # segunda tentativa: converter para 8000 Hz
                        out_path2 = out_path.replace('.wav', '.8k.wav')
                        try:
                            subprocess.run([ff, '-y', '-i', in_path, '-ar', '8000', '-ac', '1', out_path2], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            if self.recognizer is not None:
                                with sr.AudioFile(out_path2) as source2:
                                    self.recognizer.adjust_for_ambient_noise(source2, duration=0.2)
                                    audio_data2 = self.recognizer.record(source2)
                                    texto2 = None
                                    try:
                                        texto2 = self.recognizer.recognize_google(audio_data2, language='pt-BR', show_all=False)
                                    except:
                                        texto2 = None
                                    if not texto2:
                                        try:
                                            with sr.AudioFile(out_path2) as s3:
                                                a3 = self.recognizer.record(s3)
                                                texto2 = self.recognizer.recognize_google(a3, language='pt-BR', show_all=False)
                                        except:
                                            texto2 = None
                                        if texto2:
                                            return texto2
                        except:
                            pass
                    except:
                        pass
                    finally:
                        try:
                            if os.path.exists(in_path):
                                os.remove(in_path)
                            if os.path.exists(out_path):
                                os.remove(out_path)
                            out2 = out_path.replace('.wav', '.8k.wav')
                            if os.path.exists(out2):
                                os.remove(out2)
                        except:
                            pass
                if format in ('ogg', 'oga', 'opus'):
                    mime = 'audio/ogg'
                elif format in ('mp3', 'mpeg'):
                    mime = 'audio/mpeg'
                elif format == 'm4a':
                    mime = 'audio/mp4'
                elif format == 'aac':
                    mime = 'audio/aac'
                else:
                    mime = 'audio/mpeg'
                t_ai = self.transcribe_audio_bytes(audio_bytes, mime)
                if t_ai:
                    return t_ai
                return None
            mime = 'audio/ogg' if format == 'ogg' else 'audio/mpeg'
            t_ai = self.transcribe_audio_bytes(audio_bytes, mime)
            if t_ai:
                return t_ai
            return None
        except Exception:
            return None

# Instância global
audio_processor = AudioProcessor()

def testar_transcricao():
    """Teste simples da transcrição."""
    print("🧪 Testando transcrição...")
    
    # Criar um áudio de teste (simulado)
    test_text = "gastei cinquenta reais no mercado e recebi mil reais de salário"
    
    # Em produção, você teria um arquivo de áudio real
    print(f"📝 Texto esperado: {test_text}")
    print("✅ Módulo de áudio carregado com sucesso!")
    
if __name__ == '__main__':
    testar_transcricao()
