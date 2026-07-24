"""
Token Extractor Module
Extracts and manages security tokens from Facebook responses
Supports: DATR, DTSGToken (fb_dtsg), EncryptedAjaxResponseToken, and access tokens
"""

import re
import json
import logging
from typing import Optional, Dict, Any
from requests_html import HTML

logger = logging.getLogger(__name__)


class TokenExtractor:
    """Extracts various security tokens from Facebook HTML responses"""
    
    # Token patterns
    DATR_PATTERN = re.compile(r'(?<=_js_datr",")[^"]+')
    DTSG_PATTERN = re.compile(r'name="fb_dtsg"\s+value="([^"]+)"')
    ENCRYPTED_AJAX_PATTERN = re.compile(r'encrypted":"([^"]+)')
    ACCESS_TOKEN_PATTERN = re.compile(r'"accessToken":"([^"]+)"')
    REVISION_PATTERN = re.compile(r'"revision":(\d+)')
    DTI_PATTERN = re.compile(r'name="__dti"\s+value="([^"]+)"')
    
    @staticmethod
    def extract_datr_token(html_content: str) -> Optional[str]:
        """
        Extrae el token DATR del HTML
        DATR: Device Tracker token - used for device identification
        
        Args:
            html_content: Raw HTML content from Facebook response
            
        Returns:
            DATR token or None if not found
        """
        match = TokenExtractor.DATR_PATTERN.search(html_content)
        if match:
            token = match.group()
            logger.debug(f"DATR token extracted: {token[:10]}...")
            return token
        logger.warning("DATR token not found in HTML")
        return None
    
    @staticmethod
    def extract_dtsg_token(html_content: str) -> Optional[str]:
        """
        Extrae el token fb_dtsg del HTML
        DTSG: Double Token Submission Guard - CSRF protection token
        
        Args:
            html_content: Raw HTML content from Facebook response
            
        Returns:
            fb_dtsg token or None if not found
        """
        match = TokenExtractor.DTSG_PATTERN.search(html_content)
        if match:
            token = match.group(1)
            logger.debug(f"DTSG token extracted: {token[:10]}...")
            return token
        logger.warning("DTSG token not found in HTML")
        return None
    
    @staticmethod
    def extract_dti_token(html_content: str) -> Optional[str]:
        """
        Extrae el token __dti del HTML
        DTI: Device Token Info - additional device tracking
        
        Args:
            html_content: Raw HTML content from Facebook response
            
        Returns:
            __dti token or None if not found
        """
        match = TokenExtractor.DTI_PATTERN.search(html_content)
        if match:
            token = match.group(1)
            logger.debug(f"DTI token extracted: {token[:10]}...")
            return token
        logger.debug("DTI token not found in HTML")
        return None
    
    @staticmethod
    def extract_encrypted_ajax_token(html_content: str) -> Optional[str]:
        """
        Extrae el token encryptedAjaxResponseToken
        Used for AJAX requests que requieren respuestas encriptadas
        
        Args:
            html_content: Raw HTML content from Facebook response
            
        Returns:
            Encrypted AJAX token or None if not found
        """
        match = TokenExtractor.ENCRYPTED_AJAX_PATTERN.search(html_content)
        if match:
            token = match.group(1)
            logger.debug(f"Encrypted AJAX token extracted: {token[:10]}...")
            return token
        logger.debug("Encrypted AJAX token not found in HTML")
        return None
    
    @staticmethod
    def extract_access_token(html_content: str) -> Optional[str]:
        """
        Extrae el access token del HTML
        Access token for API authentication
        
        Args:
            html_content: Raw HTML content from Facebook response
            
        Returns:
            Access token or None if not found
        """
        match = TokenExtractor.ACCESS_TOKEN_PATTERN.search(html_content)
        if match:
            token = match.group(1)
            logger.debug(f"Access token extracted: {token[:10]}...")
            return token
        logger.debug("Access token not found in HTML")
        return None
    
    @staticmethod
    def extract_revision(html_content: str) -> Optional[int]:
        """
        Extrae el número de revisión
        Used for versioning and cache busting
        
        Args:
            html_content: Raw HTML content from Facebook response
            
        Returns:
            Revision number or None if not found
        """
        match = TokenExtractor.REVISION_PATTERN.search(html_content)
        if match:
            revision = int(match.group(1))
            logger.debug(f"Revision extracted: {revision}")
            return revision
        logger.debug("Revision not found in HTML")
        return None
    
    @staticmethod
    def extract_all_tokens(html_content: str) -> Dict[str, Any]:
        """
        Extrae todos los tokens disponibles del HTML
        
        Args:
            html_content: Raw HTML content from Facebook response
            
        Returns:
            Dictionary with all available tokens
        """
        tokens = {
            'datr': TokenExtractor.extract_datr_token(html_content),
            'dtsg': TokenExtractor.extract_dtsg_token(html_content),
            'dti': TokenExtractor.extract_dti_token(html_content),
            'encrypted_ajax': TokenExtractor.extract_encrypted_ajax_token(html_content),
            'access_token': TokenExtractor.extract_access_token(html_content),
            'revision': TokenExtractor.extract_revision(html_content),
        }
        
        extracted_tokens = {k: v for k, v in tokens.items() if v is not None}
        logger.info(f"Extracted {len(extracted_tokens)} tokens: {list(extracted_tokens.keys())}")
        return extracted_tokens
    
    @staticmethod
    def extract_from_html_element(element: HTML) -> Dict[str, Any]:
        """
        Extrae tokens desde un elemento HTML
        
        Args:
            element: requests_html HTML element
            
        Returns:
            Dictionary with all available tokens
        """
        if hasattr(element, 'html'):
            return TokenExtractor.extract_all_tokens(element.html)
        return {}


class TokenValidator:
    """Validates tokens for security and format"""
    
    @staticmethod
    def is_valid_datr(token: str) -> bool:
        """Valida que el token DATR tenga formato válido"""
        if not token or not isinstance(token, str):
            return False
        # DATR tokens son típicamente 32-64 caracteres alfanuméricos
        return len(token) >= 20 and re.match(r'^[a-zA-Z0-9_-]+$', token)
    
    @staticmethod
    def is_valid_dtsg(token: str) -> bool:
        """Valida que el token DTSG tenga formato válido"""
        if not token or not isinstance(token, str):
            return False
        # DTSG tokens tienen un patrón específico
        return len(token) > 10 and re.match(r'^[a-zA-Z0-9:]+$', token)
    
    @staticmethod
    def validate_all(tokens: Dict[str, Any]) -> bool:
        """
        Valida todos los tokens extraídos
        
        Returns:
            True si todos los tokens son válidos
        """
        if not tokens:
            logger.warning("No tokens provided for validation")
            return False
        
        critical_tokens = ['dtsg']  # Token mínimo requerido
        has_critical = any(token in tokens for token in critical_tokens)
        
        if not has_critical:
            logger.warning("Missing critical tokens")
            return False
        
        # Validar formato de tokens individuales
        if 'datr' in tokens and tokens['datr']:
            if not TokenValidator.is_valid_datr(tokens['datr']):
                logger.warning("Invalid DATR token format")
                return False
        
        if 'dtsg' in tokens and tokens['dtsg']:
            if not TokenValidator.is_valid_dtsg(tokens['dtsg']):
                logger.warning("Invalid DTSG token format")
                return False
        
        logger.info("All tokens validated successfully")
        return True


class TokenManager:
    """Gestiona el ciclo de vida de los tokens"""
    
    def __init__(self):
        self.tokens = {}
        self.token_timestamps = {}
    
    def store_tokens(self, tokens: Dict[str, Any]) -> bool:
        """
        Almacena tokens después de validarlos
        
        Args:
            tokens: Dictionary of tokens to store
            
        Returns:
            True if tokens were successfully stored
        """
        if not TokenValidator.validate_all(tokens):
            logger.error("Token validation failed")
            return False
        
        self.tokens.update(tokens)
        logger.info(f"Stored {len(tokens)} tokens")
        return True
    
    def get_token(self, token_name: str) -> Optional[str]:
        """Obtiene un token específico"""
        return self.tokens.get(token_name)
    
    def get_all_tokens(self) -> Dict[str, Any]:
        """Obtiene todos los tokens almacenados"""
        return self.tokens.copy()
    
    def clear_tokens(self) -> None:
        """Limpia todos los tokens almacenados"""
        self.tokens.clear()
        self.token_timestamps.clear()
        logger.info("All tokens cleared")
    
    def has_token(self, token_name: str) -> bool:
        """Verifica si existe un token específico"""
        return token_name in self.tokens and self.tokens[token_name] is not None


# Ejemplo de uso
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    # Ejemplo HTML (simulado)
    html_example = '''
    <form>
        <input name="fb_dtsg" value="AdzBbXjG5_EfPg8nnNK4" />
        <input name="__dti" value="0123456789" />
        <script>
            var data = {"accessToken":"EAAA..."};
            var _js_datr="abc123def456ghi789";
        </script>
    </form>
    '''
    
    # Extraer tokens
    tokens = TokenExtractor.extract_all_tokens(html_example)
    print(f"Extracted tokens: {tokens}")
    
    # Validar tokens
    is_valid = TokenValidator.validate_all(tokens)
    print(f"Validation result: {is_valid}")
    
    # Guardar en manager
    manager = TokenManager()
    manager.store_tokens(tokens)
    print(f"Manager tokens: {manager.get_all_tokens()}")
