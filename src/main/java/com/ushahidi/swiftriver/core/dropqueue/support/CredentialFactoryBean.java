/**
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.ushahidi.swiftriver.core.dropqueue.support;

import org.springframework.beans.factory.FactoryBean;

import com.google.api.client.auth.oauth2.BearerToken;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.http.BasicAuthentication;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;

/**
 * @author bmuita
 * 
 */
public class CredentialFactoryBean implements FactoryBean<Credential> {

	private BasicAuthentication clientAuthentication;

	private HttpTransport httpTransport;

	private JsonFactory jsonFactory;

	private String tokenServerUrl;

	public Credential getObject() throws Exception {
		TokenRequest tokenRequest = new TokenRequest(httpTransport,
				jsonFactory, new GenericUrl(tokenServerUrl),
				"client_credentials");
		tokenRequest.setClientAuthentication(clientAuthentication);

		return new Credential(BearerToken.authorizationHeaderAccessMethod())
				.setFromTokenResponse(tokenRequest.execute());
	}

	public Class<?> getObjectType() {
		return Credential.class;
	}

	public boolean isSingleton() {
		return true;
	}

	public void setClientAuthentication(BasicAuthentication clientAuthentication) {
		this.clientAuthentication = clientAuthentication;
	}

	public void setHttpTransport(HttpTransport httpTransport) {
		this.httpTransport = httpTransport;
	}

	public void setJsonFactory(JsonFactory jsonFactory) {
		this.jsonFactory = jsonFactory;
	}

	public void setTokenServerUrl(String tokenServerUrl) {
		this.tokenServerUrl = tokenServerUrl;
	}

}
