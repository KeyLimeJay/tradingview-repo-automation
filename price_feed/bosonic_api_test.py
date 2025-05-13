#!/usr/bin/env python3
"""
Bosonic API tester - trying different endpoints and authentication methods
"""
import requests
import json
import sys
import time
import datetime

class BosonicApiTester:
    def __init__(self):
        # Bosonic API credentials
        self.username = "tv-algo-1@bosonic.digital"
        self.password = "Prod1234$"
        self.api_code = "NWY3YTFjODExODUwODQ3N2I3MzUzYjcz"
        self.base_url = "https://trad6.bosonic.digital"
        
        # Session
        self.session = requests.Session()
        self.auth_token = None
        
        print("Bosonic API tester initialized")
    
    def test_endpoints(self):
        """Test various endpoints to find the correct authentication method"""
        print("Testing different API endpoints...")
        
        # List of potential endpoints to try
        endpoints = [
            "/login",
            "/api/login",
            "/api/v1/login",
            "/trader/login",
            "/auth/login",
            "/oauth/token",
            "/api/token"
        ]
        
        # Try different authentication methods
        for endpoint in endpoints:
            url = f"{self.base_url}{endpoint}"
            print(f"\nTrying endpoint: {url}")
            
            # Try POST with JSON
            try:
                payload = {
                    "username": self.username,
                    "password": self.password,
                    "code": self.api_code
                }
                response = self.session.post(url, json=payload, timeout=5)
                print(f"POST with JSON - Status: {response.status_code}")
                if response.status_code < 400:  # Success
                    print(f"Response: {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"POST with JSON failed: {e}")
            
            # Try POST with form data
            try:
                form_data = {
                    "username": self.username,
                    "password": self.password,
                    "code": self.api_code
                }
                response = self.session.post(url, data=form_data, timeout=5)
                print(f"POST with form data - Status: {response.status_code}")
                if response.status_code < 400:  # Success
                    print(f"Response: {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"POST with form data failed: {e}")
            
            # Try Basic Auth
            try:
                response = self.session.get(url, auth=(self.username, self.password), timeout=5)
                print(f"GET with Basic Auth - Status: {response.status_code}")
                if response.status_code < 400:  # Success
                    print(f"Response: {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"GET with Basic Auth failed: {e}")
    
    def test_site_structure(self):
        """Test the site structure to understand the API"""
        print("\nTesting site structure...")
        
        # Common endpoint patterns to check
        endpoints = [
            "/",
            "/api",
            "/docs",
            "/swagger",
            "/openapi.json",
            "/help",
            "/explorer"
        ]
        
        for endpoint in endpoints:
            url = f"{self.base_url}{endpoint}"
            try:
                response = self.session.get(url, timeout=5)
                print(f"GET {url} - Status: {response.status_code}")
                if response.status_code < 400:
                    # Try to determine if it's HTML or JSON
                    content_type = response.headers.get('Content-Type', '')
                    if 'json' in content_type.lower():
                        try:
                            data = response.json()
                            print(f"JSON response: {json.dumps(data, indent=2)[:500]}...")  # Limit output
                        except:
                            print("Response appears to be JSON but couldn't be parsed")
                    elif 'html' in content_type.lower():
                        print("HTML response detected (not showing content)")
                    else:
                        print(f"Content-Type: {content_type}")
                        print(f"First 500 chars: {response.text[:500]}...")
            except requests.exceptions.RequestException as e:
                print(f"GET {url} failed: {e}")

def main():
    tester = BosonicApiTester()
    
    # Test endpoints
    tester.test_endpoints()
    
    # Test site structure
    tester.test_site_structure()
    
    print("\nAPI testing complete")

if __name__ == "__main__":
    main()
